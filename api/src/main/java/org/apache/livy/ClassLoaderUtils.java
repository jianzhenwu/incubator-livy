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

package org.apache.livy;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ClassLoaderUtils {

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded by this class.
   */
  public static ClassLoader getContextOrDefaultClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ClassLoaderUtils.class.getClassLoader();
    }
    return classLoader;
  }

  /**
   * Load file from classpath by file name. Return null if file is not found.
   */
  public static Properties loadAsPropertiesFromClasspath(String name) throws IOException {
    URL url = getContextOrDefaultClassLoader().getResource(name);
    Properties config = new Properties();
    if (url == null) {
      return null;
    }

    try (Reader r = new InputStreamReader(url.openStream(), UTF_8)) {
      config.load(r);
    }
    return config;
  }
}
