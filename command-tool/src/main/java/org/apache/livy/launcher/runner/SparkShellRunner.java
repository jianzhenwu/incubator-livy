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

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.ScalaInterpreter;
import org.apache.livy.launcher.LivyOption;

public class SparkShellRunner extends AbstractInteractiveRunner {

  private static final Logger logger =
      LoggerFactory.getLogger(SparkShellRunner.class);

  private static final String SCALA_PROMPT_START = "scala>";
  /**
   * The lengths of SCALA_PROMPT_CONTINUE string is the same as SCALA_PROMPT_START.
   */
  private static final String SCALA_PROMPT_CONTINUE = "     |";

  public SparkShellRunner(LivyOption livyOption) {
    super(livyOption);
  }

  @Override
  public void handleLine(String line) {
    builder.append(line);
    builder.append("\n");

    String code = builder.toString();
    if (StringUtils.isBlank(code.trim())) {
      prompt = SCALA_PROMPT_START;
      return;
    }
    String result = ScalaInterpreter.parse(code);
    if (!ScalaInterpreter.Incomplete().equals(result)) {
      List<List<String>> res = restClient.runStatement(code);
      outputStatementResult(res);
      builder.setLength(0);
      prompt = SCALA_PROMPT_START;
      return;
    }
    prompt = SCALA_PROMPT_CONTINUE;
  }

  @Override
  public String promptStart(){
    return SCALA_PROMPT_START;
  }

  @Override
  public String promptContinue() {
    return SCALA_PROMPT_CONTINUE;
  }

  @Override
  protected Logger logger() {
    return logger;
  }
}
