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
package org.apache.livy.jupyter.adapters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class ListAdapter<T> extends TypeAdapter<List<T>> {
  private Class<T> clazz;
  private Gson gson;

  public ListAdapter(Class<T> clazz, Gson gson) {
    this.clazz = clazz;
    this.gson = gson;
  }

  @Override
  public void write(JsonWriter out, List<T> values) throws IOException {
    if (values == null) {
      out.beginArray();
      out.endArray();
      return;
    }
    TypeAdapter<T> adapter = gson.getAdapter(clazz);
    out.beginArray();
    for (T elem : values) {
      adapter.write(out, elem);
    }
    out.endArray();
  }

  @Override
  public List read(JsonReader in) throws IOException {
    List<T> res = new ArrayList<>();
    switch (in.peek()) {
      case BEGIN_OBJECT: res.add(gson.fromJson(in, clazz)); break;
      case BEGIN_ARRAY: {
        in.beginArray();
        while (in.hasNext()) {
          res.add(gson.fromJson(in, clazz));
        }
        in.endArray();
      }
      default:
    }
    return res;
  }
}
