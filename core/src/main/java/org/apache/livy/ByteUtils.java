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

package org.apache.livy;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteUtils {

  private static final Map<String, ByteUnit> byteSuffixes =
      new HashMap<String, ByteUnit>() {{
        put("b", ByteUnit.BYTE);
        put("k", ByteUnit.KiB);
        put("kb", ByteUnit.KiB);
        put("m", ByteUnit.MiB);
        put("mb", ByteUnit.MiB);
        put("g", ByteUnit.GiB);
        put("gb", ByteUnit.GiB);
        put("t", ByteUnit.TiB);
        put("tb", ByteUnit.TiB);
        put("p", ByteUnit.PiB);
        put("pb", ByteUnit.PiB);
      }};

  /**
   * Convert a passed byte string (e.g. 50b, 100kb, or 250mb) to the given.
   * If no suffix is provided, a direct conversion to the provided unit is attempted.
   */
  public static long byteStringAs(String str, ByteUnit unit) {
    String lower = str.toLowerCase(Locale.ROOT).trim();

    try {
      Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
      Matcher fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower);

      if (m.matches()) {
        long val = Long.parseLong(m.group(1));
        String suffix = m.group(2);

        // Check for invalid suffixes
        if (suffix != null && !byteSuffixes.containsKey(suffix)) {
          throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
        }

        // If suffix is valid use that, otherwise none was provided and use the default passed
        return unit.convertFrom(val, suffix != null ? byteSuffixes.get(suffix) : unit);
      } else if (fractionMatcher.matches()) {
        throw new NumberFormatException("Fractional values are not supported. Input was: "
            + fractionMatcher.group(1));
      } else {
        throw new NumberFormatException("Failed to parse byte string: " + str);
      }

    } catch (NumberFormatException e) {
      String byteError = "Size must be specified as bytes (b), " +
          "kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). " +
          "E.g. 50b, 100k, or 250m.";

      throw new NumberFormatException(byteError + "\n" + e.getMessage());
    }
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for internal use.
   * If no suffix is provided, the passed number is assumed to be in bytes.
   */
  public static long byteStringAsBytes(String str) {
    return byteStringAs(str, ByteUnit.BYTE);
  }
}
