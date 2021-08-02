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

package org.apache.livy.thriftserver.session;

import java.io.StringWriter;
import java.sql.Timestamp;

/**
 * Utility class used for transferring process status to the Livy server.
 */
public class JobProcess {
    private final int allTask;
    private final int completedTask;
    private final int activeTask;
    private final int failedTask;
    private final String PROCESS_FORMAT = "%-23s %-60s (%s + %s) / %s";
    private static final int PROGRESS_BAR_CHARS = 60;

    // [==================>    ]
    private String getInPlaceProgressBar(double percent) {
        StringWriter bar = new StringWriter();
        bar.append("[");
        int remainingChars = PROGRESS_BAR_CHARS - 4;
        int completed = (int) (remainingChars * percent);
        int pending = remainingChars - completed;
        for (int i = 0; i < completed; i++) {
            bar.append("=");
        }
        bar.append(">");
        for (int i = 0; i < pending; i++) {
            bar.append(" ");
        }
        bar.append("]");
        return bar.toString();
    }

    public JobProcess() {
        this(0, 0, 0, 0);
    }

    public JobProcess(int allTask, int completedTask, int activeTask, int failedTask) {
        this.allTask = allTask;
        this.completedTask = completedTask;
        this.activeTask = activeTask;
        this.failedTask = failedTask;
    }

    @Override
    public String toString() {
        double percentage =  this.completedTask / (double) this.allTask;
        return String.format(
                PROCESS_FORMAT,
                new Timestamp(System.currentTimeMillis()),
                getInPlaceProgressBar(percentage),
                this.completedTask,
                this.activeTask,
                this.allTask);
    }
}