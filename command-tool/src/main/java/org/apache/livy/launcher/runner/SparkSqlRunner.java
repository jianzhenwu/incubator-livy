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
package org.apache.livy.launcher.runner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.Utils;
import org.apache.livy.client.http.response.StatementResponse;
import org.apache.livy.launcher.LivyOption;
import org.apache.livy.launcher.SparkSqlOption;
import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;

public class SparkSqlRunner extends AbstractInteractiveRunner {

  public static final String SQL_PROMPT_START = "spark-sql>";
  // The length of SQL_PROMPT_CONTINUE string is the same as SQL_PROMPT_START.
  public static final String SQL_PROMPT_CONTINUE = "         >";
  private static final Logger logger =
      LoggerFactory.getLogger(SparkSqlRunner.class);
  private final Map<String, String> hiveVar = new HashMap<>();

  public SparkSqlRunner(LivyOption livyOption) {
    super(livyOption);
    this.exitIfQueryExecuted((SparkSqlOption) livyOption);
  }

  private void exitIfQueryExecuted(SparkSqlOption sparkSqlOption) {

    for (Map.Entry<Object, Object> e : sparkSqlOption.getDefineProperties()
        .entrySet()) {
      this.hiveVar.put(e.getKey().toString(), e.getValue().toString());
    }

    for (Map.Entry<Object, Object> e : sparkSqlOption.getHiveVar().entrySet()) {
      this.hiveVar.put(e.getKey().toString(), e.getValue().toString());
    }

    String scriptFile = sparkSqlOption.getSqlFile();
    String[] initFiles = sparkSqlOption.getInitFiles();
    String dbName = sparkSqlOption.getDatabase();
    String queryString = sparkSqlOption.getQuery();

    this.executeSetHiveVar(this.hiveVar);

    if (StringUtils.isNotBlank(dbName)) {
      runStatement(String.format("use %s ", dbName));
    }

    if (initFiles != null && initFiles.length > 0) {
      for (String initFile : initFiles) {
        this.executeSqlFromFile(initFile);
      }
    }

    if (StringUtils.isNotBlank(queryString)) {
      this.executeSqlBuffer(queryString);
      this.exit = true;
    }

    if (StringUtils.isNotBlank(scriptFile)) {
      this.executeSqlFromFile(scriptFile);
      this.exit = true;
    }
  }

  private void executeSetHiveVar(Map<String, String> properties) {
    if (!properties.isEmpty()) {
      Set<Map.Entry<String, String>> entries = properties.entrySet();
      for (Map.Entry<String, String> e : entries) {
        String code =
            String.format("set hivevar:%s=%s", e.getKey(), e.getValue());
        runStatement(code);
      }
    }
  }

  private void executeSqlFromFile(String filePath) {
    StringBuilder buf = new StringBuilder();
    try(BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(filePath)))) {
      String line;
      while ((line = reader.readLine()) != null) {
        buf.append(line).append("\n");
      }
      if (buf.length() > 0) {
        this.executeSqlBuffer(buf.toString());
      }
    } catch (IOException e) {
      logger.error("Fail to execute sql file {} ", filePath, e);
      throw new LivyLauncherException(LauncherExitCode.others);
    }
  }

  @Override
  public void addComplete(ConsoleReader consoleReader) {
    Set<String> completions = new TreeSet<>();
    StringBuilder builder = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        Objects.requireNonNull(SparkSqlRunner.class.getClassLoader()
            .getResourceAsStream("sql-keywords.txt"))))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        builder.append(line).append("\n");
      }
    } catch (IOException io) {
      logger.error("Fail to load completeFile for consoleReader.");
    }

    String keywords = builder.toString();
    // also allow lower-case versions of all the keywords
    keywords += "," + keywords.toLowerCase();
    StringTokenizer tok = new StringTokenizer(keywords, ", ");
    while (tok.hasMoreTokens()) {
      completions.add(tok.nextToken());
    }

    consoleReader.addCompleter(new ArgumentCompleter(
        new ArgumentCompleter.AbstractArgumentDelimiter() {
          @Override
          public boolean isDelimiterChar(CharSequence buffer, int pos) {
            char c = buffer.charAt(pos);
            if (Character.isWhitespace(c)) {
              return true;
            }
            return !(Character.isLetterOrDigit(c)) && c != '_';
          }
        }, new StringsCompleter(completions)));
  }

  @Override
  public void handleLine(String line) {
    line = line.trim();
    builder.append(line);
    builder.append("\n");
    if (line.endsWith(";") && !line.endsWith("\\;")) {
      executeSqlBuffer(builder.toString());
      builder.setLength(0);
      prompt = SQL_PROMPT_START;
    } else {
      prompt = SQL_PROMPT_CONTINUE;
    }
  }

  @Override
  public String promptStart() {
    return SQL_PROMPT_START;
  }

  @Override
  public String promptContinue() {
    return SQL_PROMPT_CONTINUE;
  }

  private void executeSqlBuffer(String line) {
    List<String> sqls = Utils.processLine(line);
    for (String sql : sqls) {
      StatementResponse statementResponse = runStatement(sql);
      handleStatementResponse(statementResponse);
    }
  }

  @Override
  public Logger logger() {
    return logger;
  }
}
