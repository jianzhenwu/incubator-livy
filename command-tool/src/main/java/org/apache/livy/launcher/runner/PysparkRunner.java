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
import org.python.antlr.base.mod;
import org.python.core.CompileMode;
import org.python.core.CompilerFlags;
import org.python.core.ParserFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.launcher.LivyOption;

public class PysparkRunner extends AbstractInteractiveRunner {

  private static final Logger logger =
      LoggerFactory.getLogger(PysparkRunner.class);

  public static final String PYTHON_PROMPT_START = ">>>";
  public static final String PYTHON_PROMPT_CONTINUE = "...";

  public PysparkRunner(LivyOption livyOption) {
    super(livyOption);
  }

  @Override
  public void handleLine(String line) {
    builder.append(line);
    builder.append("\n");

    String code = builder.toString();
    if (StringUtils.isBlank(code.trim())) {
      prompt = PYTHON_PROMPT_START;
      return;
    }
    try {
      mod node = ParserFacade.partialParse(code, CompileMode.single, "<stdin>",
          new CompilerFlags(), true);
      // Indicate a partial statement was entered.
      if (node == null) {
        prompt = PYTHON_PROMPT_CONTINUE;
        return;
      }
    } catch (Exception e) {
      // Run statement and print error info.
    }
    List<List<String>> res = restClient.runStatement(code);
    outputStatementResult(res);
    builder.setLength(0);
    prompt = PYTHON_PROMPT_START;
  }

  @Override
  public String promptStart() {
    return PYTHON_PROMPT_START;
  }

  @Override
  public String promptContinue() {
    return PYTHON_PROMPT_CONTINUE;
  }

  @Override
  protected Logger logger() {
    return logger;
  }
}
