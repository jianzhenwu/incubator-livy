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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.launcher.exception.LauncherExitCode;
import org.apache.livy.launcher.exception.LivyLauncherException;

public class CipherUtils {

  private static final Logger logger = LoggerFactory.getLogger(CipherUtils.class);

  private static final String KEY = "E5E9FA1BA31ECD1AE84F75CAAA474F3A";

  private static final String cipherName = "AES";

  private static final String algorithm = "MD5";

  public static String encrypt(String value) {
    byte[] encrypted = doCipherOp(Cipher.ENCRYPT_MODE, value.getBytes(StandardCharsets.UTF_8));
    return Base64.encodeBase64String(encrypted);
  }

  public static String decrypt(String value) {
    byte[] encrypted = Base64.decodeBase64(value);
    byte[] decrypted = doCipherOp(Cipher.DECRYPT_MODE, encrypted);
    return new String(decrypted, StandardCharsets.UTF_8);
  }

  private static byte[] getSecretKey(String secret) throws NoSuchAlgorithmException {
    return MessageDigest.getInstance(CipherUtils.algorithm).digest(secret.getBytes());
  }

  private static byte[] doCipherOp(int mode, byte[] in) {
    try {
      Cipher cipher = Cipher.getInstance(CipherUtils.cipherName);
      SecretKeySpec secretKeySpec =
          new SecretKeySpec(getSecretKey(KEY), CipherUtils.cipherName);
      if (mode != Cipher.ENCRYPT_MODE && mode != Cipher.DECRYPT_MODE) {
        throw new IllegalArgumentException(String.valueOf(mode));
      }
      cipher.init(mode, secretKeySpec);
      return cipher.doFinal(in);
    } catch (Exception e) {
      logger.error("Failed to do cipher operation.", e);
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage());
    }
  }
}
