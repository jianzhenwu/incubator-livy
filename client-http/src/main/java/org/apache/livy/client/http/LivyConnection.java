/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.client.http;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import org.apache.livy.client.http.exception.AuthServerException;
import org.apache.livy.client.http.exception.ServiceUnavailableException;
import static org.apache.livy.client.http.HttpConf.Entry.*;
import static org.apache.livy.client.http.SessionType.Interactive;

/**
 * Abstracts a connection to the Livy server; serializes multiple requests so that we only need
 * one active HTTP connection (to keep resource usage down).
 */
class LivyConnection {

  private static final String APPLICATION_JSON = "application/json";

  private final URI server;
  private final String uriRoot;
  private final CloseableHttpClient client;
  private final ObjectMapper mapper;


  LivyConnection(URI uri, final HttpConf config) {
    this(uri, config, Interactive);
  }

  LivyConnection(URI uri, final HttpConf config, SessionType sessionType) {
    HttpClientContext ctx = HttpClientContext.create();
    int port = uri.getPort() > 0 ? uri.getPort() : 8998;

    String path = uri.getPath() != null ? uri.getPath() : "";
    this.uriRoot = path + "/" + sessionType.getSessionType();

    RequestConfig reqConfig = new RequestConfig() {
      @Override
      public int getConnectTimeout() {
        return (int) config.getTimeAsMs(CONNECTION_TIMEOUT);
      }

      @Override
      public int getSocketTimeout() {
        return (int) config.getTimeAsMs(SOCKET_TIMEOUT);
      }

      @Override
      public boolean isAuthenticationEnabled() {
        return true;
      }

      @Override
      public boolean isContentCompressionEnabled() {
        return config.getBoolean(CONTENT_COMPRESS_ENABLE);
      }

      @Override
      public boolean isExpectContinueEnabled() {
        return true;
      }
    };

    Credentials credentials;
    // If user info is specified in the url, pass them to the CredentialsProvider.
    if (uri.getUserInfo() != null) {
      String[] userInfo = uri.getUserInfo().split(":");
      if (userInfo.length < 1) {
        throw new IllegalArgumentException("Malformed user info in the url.");
      }
      try {
        String username = URLDecoder.decode(userInfo[0], StandardCharsets.UTF_8.name());
        String password = "";
        if (userInfo.length > 1) {
          password = URLDecoder.decode(userInfo[1], StandardCharsets.UTF_8.name());
        }
        credentials = new UsernamePasswordCredentials(username, password);
      } catch (Exception e) {
        throw new IllegalArgumentException("User info in the url contains bad characters.", e);
      }
    } else {
      credentials = new Credentials() {
        @Override
        public String getPassword() {
          return null;
        }

        @Override
        public Principal getUserPrincipal() {
          return null;
        }
      };
    }

    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(AuthScope.ANY, credentials);

    PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
    manager.setMaxTotal(2);

    HttpClientBuilder builder = HttpClientBuilder.create()
      .setRedirectStrategy(new LaxRedirectStrategy())
      .evictExpiredConnections()
      .evictIdleConnections(config.getTimeAsMs(CONNECTION_IDLE_TIMEOUT), TimeUnit.MILLISECONDS)
      .setConnectionManager(manager)
      .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
      .setDefaultRequestConfig(reqConfig)
      .setDefaultCredentialsProvider(credsProvider)
      .setUserAgent("livy-client-http");

    if (config.isSpnegoEnabled()) {
      Registry<AuthSchemeProvider> authSchemeProviderRegistry =
        RegistryBuilder.<AuthSchemeProvider>create()
          .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory())
          .build();
      builder.setDefaultAuthSchemeRegistry(authSchemeProviderRegistry);
    }

    this.server = uri;
    this.client = builder.build();
    this.mapper = new ObjectMapper();
  }

  synchronized void close() throws IOException {
    client.close();
  }

  synchronized <V> V delete(Class<V> retType, String uri, Object... uriParams) throws Exception {
    return sendJSONRequest(new HttpDelete(), retType, uri, uriParams);
  }

  synchronized <V> V get(Class<V> retType, String uri, Object... uriParams) throws Exception {
    return sendJSONRequest(new HttpGet(), retType, uri, uriParams);
  }

  synchronized <V> V get(Class<V> retType, String uri, String query,
      Object... uriParams) throws Exception {
    return sendJSONRequest(new HttpGet(), retType, uri, query, uriParams);
  }

  synchronized <V> V post(
      Object body,
      Class<V> retType,
      String uri,
      Object... uriParams) throws Exception {
    HttpPost post = new HttpPost();
    if (body != null) {
      byte[] bodyBytes = mapper.writeValueAsBytes(body);
      post.setEntity(new ByteArrayEntity(bodyBytes));
    }
    return sendJSONRequest(post, retType, uri, uriParams);
  }

  synchronized <V> V post(
      File f,
      Class<V> retType,
      String paramName,
      String uri,
      Object... uriParams) throws Exception {
    HttpPost post = new HttpPost();
    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    builder.addPart(paramName, new FileBody(f));
    post.setEntity(builder.build());
    return sendRequest(post, retType, uri, uriParams);
  }

  private <V> V sendJSONRequest(
      HttpRequestBase req,
      Class<V> retType,
      String uri,
      Object... uriParams) throws Exception {
    return sendJSONRequest(req, retType, uri, null, uriParams);
  }

  private <V> V sendJSONRequest(
      HttpRequestBase req,
      Class<V> retType,
      String uri,
      String query,
      Object... uriParams) throws Exception {
    req.setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON);
    req.setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
    req.setHeader(HttpHeaders.CONTENT_ENCODING, "UTF-8");
    return sendRequest(req, retType, uri, query, uriParams);
  }

  private <V> V sendRequest(
      HttpRequestBase req,
      Class<V> retType,
      String uri,
      Object... uriParams) throws Exception {
    return this.sendRequest(req, retType, uri, null, uriParams);
  }

  private <V> V sendRequest(
      HttpRequestBase req,
      Class<V> retType,
      String uri,
      String query,
      Object... uriParams) throws Exception {
    req.setURI(
        new URI(server.getScheme(), server.getUserInfo(), server.getHost(),
            server.getPort(), uriRoot + String.format(uri, uriParams),
            query, null));
    // It is no harm to set X-Requested-By when csrf protection is disabled.
    if (req instanceof HttpPost || req instanceof HttpDelete || req instanceof HttpPut
            || req instanceof  HttpPatch) {
      req.addHeader("X-Requested-By", "livy");
    }
    if (StringUtils.isNotBlank(server.getUserInfo())) {
      req.addHeader("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(URLDecoder
              .decode(server.getUserInfo(), StandardCharsets.UTF_8.toString())
              .getBytes(StandardCharsets.UTF_8)));
    }
    try (CloseableHttpResponse res = client.execute(req)) {
      int statusCode = res.getStatusLine().getStatusCode();
      int status = (statusCode / 100) * 100;
      HttpEntity entity = res.getEntity();
      if (status == HttpStatus.SC_OK) {
        if (!Void.class.equals(retType)) {
          return mapper.readValue(entity.getContent(), retType);
        } else {
          return null;
        }
      } else {
        String error = EntityUtils.toString(entity);
        if (statusCode == HttpStatus.SC_BAD_GATEWAY
            || statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE
            || statusCode == HttpStatus.SC_GATEWAY_TIMEOUT) {
          throw new ServiceUnavailableException(String
              .format("%s: %s", res.getStatusLine().getReasonPhrase(), error));
        }
        if (statusCode == HttpStatus.SC_INTERNAL_SERVER_ERROR
            && error != null
            && error.contains("Internal authentication server error")) {
          throw new AuthServerException(error);
        }
        throw new IOException(String.format("%s: %s", res.getStatusLine().getReasonPhrase(),
          error));
      }
    }
  }

}
