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

package org.apache.livy.launcher;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.launcher.exception.LivyLauncherException;

public class SparkSubmitOption extends LivyOption {

  private static final Logger logger =
      LoggerFactory.getLogger(SparkSubmitOption.class);

  public SparkSubmitOption(List<String> args) {
    super(args, "", new DefaultExtraArgsParser());
  }

  /**
   * In fact it is not need to use DefaultExtraArgsParser in sparkSubmit,
   * so this method is overridden.
   * @param extraArgs User application main args.
   */
  @Override
  protected void handleExtraArgs(List<String> extraArgs) {
    if (extraArgs.size() < 1) {
      return;
    }
    String primaryResource = extraArgs.get(0);
    this.setFile(primaryResource);

    if (extraArgs.size() > 1) {
      setExtraArgs(extraArgs.subList(1, extraArgs.size()));
    }
  }

  @Override
  protected void validateOptions(List<String> args) {
    super.validateOptions(args);

    if (this.getFile() == null) {
      logger.error("Must specify a primary resource (JAR or Python or R file)");
      throw new LivyLauncherException(1);
    }
  }
}
