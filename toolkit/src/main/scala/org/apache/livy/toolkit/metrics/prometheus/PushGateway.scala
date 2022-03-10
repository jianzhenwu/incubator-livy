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


package org.apache.livy.toolkit.metrics.prometheus

import java.io.{BufferedWriter, Closeable, IOException, OutputStreamWriter}
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import javax.ws.rs.HttpMethod
import javax.ws.rs.core.HttpHeaders

import com.codahale.metrics._

import org.apache.livy.Logging
import org.apache.livy.toolkit.metrics.common.TextFormat
import org.apache.livy.toolkit.metrics.prometheus.writer.{MetricsWriter, PrometheusTextWriter}

class PushGateway() extends Closeable with Logging {
  private var appName = ""
  private var appId = ""
  private var queue = ""
  private var url = ""
  var targetUrl = ""
  private var token = ""
  private val SECONDS_PER_MILLISECOND = 1000
  @volatile private var connection: HttpURLConnection = _
  private var textWriter: PrometheusTextWriter = _
  private var metricsWriter: MetricsWriter = _

  @throws[IOException]
  def connect(): Unit = {
    if (!isConnected) {
      val conn: HttpURLConnection = new URL(targetUrl).openConnection
        .asInstanceOf[HttpURLConnection]
      conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, TextFormat.REQUEST_CONTENT_TYPE)
      conn.setRequestProperty("Authorization", "Basic " + token)
      conn.setDoOutput(true)
      conn.setRequestMethod(HttpMethod.POST)
      conn.setConnectTimeout(10 * SECONDS_PER_MILLISECOND)
      conn.setReadTimeout(10 * SECONDS_PER_MILLISECOND)
      conn.connect()
      this.textWriter = new PrometheusTextWriter(new BufferedWriter(
        new OutputStreamWriter(conn.getOutputStream, StandardCharsets.UTF_8))
      )
      this.metricsWriter = new MetricsWriter(textWriter)
      this.connection = conn
    }
  }

  @throws[IOException]
  def close(): Unit = {
    try if (textWriter != null) textWriter.close() catch {
      case e: IOException => error("Error closing textWriter", e)
    } finally {
      this.textWriter = null
      this.metricsWriter = null
    }

    val response: Int = connection.getResponseCode()
    if (response != HttpURLConnection.HTTP_ACCEPTED) throw new IOException("Response code from " +
      targetUrl + " was " + response)
    connection.disconnect()
    this.connection = null
  }

  @throws[IOException]
  def sendCounter(name: String, counter: Counter): Unit = {
    metricsWriter.writeCounter(name, counter)
  }

  @throws[IOException]
  def sendGauge[T](name: String, gauge: Gauge[T]): Unit = {
    metricsWriter.writeGauge(name, gauge)
  }

  @throws[IOException]
  def sendTime(name: String, timer: Timer): Unit = {
    metricsWriter.writeTimer(name, timer)
  }

  def isConnected(): Boolean = {
    connection != null
  }

  @throws[IOException]
  def flush(): Unit = {
    if (textWriter != null) {
      textWriter.flush()
    }
  }

  def setAppName(appName: String): PushGateway = {
    this.appName = appName
    this
  }

  def setAppId(appId: String): PushGateway = {
    this.appId = appId
    this
  }

  def setQueueName(queue: String): PushGateway = {
    this.queue = queue
    this
  }

  def setPushUrl(url: String): PushGateway = {
    this.url = url
    this
  }

  def setPushToken(token: String): PushGateway = {
    this.token = token
    this
  }

  def buildTargetUrl(): PushGateway = {
    targetUrl = url + "/metrics/job/" + URLEncoder.encode(appId, StandardCharsets.UTF_8.name) +
    "/queue/" + URLEncoder.encode(queue, StandardCharsets.UTF_8.name) +
    "/appName/" + URLEncoder.encode(appName, StandardCharsets.UTF_8.name)
    this
  }
}
