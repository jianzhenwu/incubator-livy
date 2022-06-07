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

package org.apache.livy.client.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LauncherConf extends ClientConf<LauncherConf> {

  private static final String LAUNCHER_CONF_PREFIX = "livy.launcher.";

  public static enum Entry implements ConfEntry {

    SESSION_CREATE_TIMEOUT("session.create.timeout", "5m"),
    STATEMENT_TIMEOUT("statement.timeout", "0s"),
    STATEMENT_POLLING_INTERVAL_OFFSET("statement.polling.interval.offset", "100ms"),
    STATEMENT_POLLING_INTERVAL_STEP("statement.pooling.interval.step", "100ms"),
    STATEMENT_POLLING_INTERVAL_MAX("statement.pooling.interval.max", "1s"),

    SESSION_CREATE_PRINT_LOG("session.create.print-log", true),
    SESSION_TRACKING_URL("session.tracking-url", null),

    FS_S3A_ENABLED("fs.s3a.enabled", true),
    SESSION_STAGING_DIR("session.staging-dir", null);

    private final String key;
    private final Object dflt;

    Entry(String key, Object dflt) {
      this.key = LAUNCHER_CONF_PREFIX + key;
      this.dflt = dflt;
    }

    @Override
    public String key() { return key; }

    @Override
    public Object dflt() { return dflt; }
  }

  public LauncherConf(Properties config) {
    super(config);
  }

  @Override
  protected Map<String, DeprecatedConf> getConfigsWithAlternatives() {
    return Collections.unmodifiableMap(new HashMap<>());
  }

  @Override
  protected Map<String, DeprecatedConf> getDeprecatedConfigs() {
    return Collections.unmodifiableMap(new HashMap<>());
  }
}
