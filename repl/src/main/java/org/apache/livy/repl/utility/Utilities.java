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

package org.apache.livy.repl.utility;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

public class Utilities {
  private static Map<String, Utility> utils = new HashMap<String, Utility>();

  static {
    ServiceLoader<Utility> sl = ServiceLoader.load(Utility.class);
    Iterator<Utility> utilities = sl.iterator();
    while (utilities.hasNext()) {
      Utility utility = utilities.next();
      utils.put(utility.name(), utility);
    }
  }

  public static Object get(String name) {
    return utils.get(name);
  }
}

interface Utility {
  String name();
}
