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

package org.apache.livy.launcher.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.ini4j.Ini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.client.common.StatementState;
import org.apache.livy.launcher.exception.LauncherExitCode;

import static org.apache.livy.launcher.LivyLauncherConfiguration.HADOOP_USER_RPCPASSWORD;

public class LauncherUtils {

  private static final Logger logger = LoggerFactory.getLogger(LauncherUtils.class);

  public static List<String> splitByComma(String str) {
    List<String> list = new ArrayList<>();
    if (StringUtils.isNotBlank(str)) {
      String[] strs = str.split(",");
      for (String s : strs) {
        list.add(s.trim());
      }
    }
    return list;
  }

  public static Integer firstInteger(String... nums) {
    for (String num : nums) {
      if (StringUtils.isNotBlank(num)) {
        return Integer.valueOf(num.trim());
      }
    }
    return null;
  }

  public static String passwordFromSDI() throws IOException {
    String sdiFile =
        String.format("%s/.sdi/credentials", System.getProperty("user.home"));

    logger.debug("Read sdi credentials from {}", sdiFile);
    if (new File(sdiFile).isFile()) {
      try (FileInputStream fileInputStream = new FileInputStream(sdiFile)) {
        return new Ini(fileInputStream).get("default", HADOOP_USER_RPCPASSWORD);
      }
    }
    return null;
  }

  public static LauncherExitCode statementExitCode(String state,
      String outputStatus) {

    if (StatementState.Available.toString().equalsIgnoreCase(state) && "ok"
        .equalsIgnoreCase(outputStatus)) {
      return LauncherExitCode.normal;
    }
    return LauncherExitCode.appError;
  }

  public static LauncherExitCode batchSessionExitCode(String state) {
    return "success".equalsIgnoreCase(state) ? LauncherExitCode.normal :
        LauncherExitCode.appError;
  }
}
