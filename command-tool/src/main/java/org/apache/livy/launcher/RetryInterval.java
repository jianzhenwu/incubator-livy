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

public class RetryInterval implements Interval {

  private final long offset;
  private final long step;
  private final long max;

  public RetryInterval(long offset, long step, long max) {
    this.offset = offset;
    this.step = step;
    this.max = max;
  }

  @Override
  public long interval(int count) {
    double t = Math.log(count + 1) * step + offset;
    return Double.valueOf(Math.min(t, max)).longValue();
  }
}
