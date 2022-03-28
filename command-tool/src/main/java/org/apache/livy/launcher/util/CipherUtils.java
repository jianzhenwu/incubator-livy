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

  private static final String cipherName = "AES";

  private static final String algorithm = "MD5";

  public static String encrypt(String secret, String value) {
    byte[] encrypted = doCipherOp(Cipher.ENCRYPT_MODE, secret,
        value.getBytes(StandardCharsets.UTF_8));
    return Base64.encodeBase64String(encrypted);
  }

  public static String decrypt(String secret, String value) {
    byte[] encrypted = Base64.decodeBase64(value);
    byte[] decrypted = doCipherOp(Cipher.DECRYPT_MODE, secret, encrypted);
    return new String(decrypted, StandardCharsets.UTF_8);
  }

  private static byte[] getSecretKey(String secret) throws NoSuchAlgorithmException {
    return MessageDigest.getInstance(CipherUtils.algorithm).digest(secret.getBytes());
  }

  private static byte[] doCipherOp(int mode, String secret, byte[] in) {
    try {
      Cipher cipher = Cipher.getInstance(CipherUtils.cipherName);
      SecretKeySpec secretKeySpec =
          new SecretKeySpec(getSecretKey(secret), CipherUtils.cipherName);
      switch (mode) {
        case Cipher.ENCRYPT_MODE:
          cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
          break;
        case Cipher.DECRYPT_MODE:
          cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
          break;
        default:
          throw new IllegalArgumentException(String.valueOf(mode));
      }
      return cipher.doFinal(in);
    } catch (Exception e) {
      logger.error("Failed to do cipher operation.", e);
      throw new LivyLauncherException(LauncherExitCode.others, e.getMessage());
    }
  }
}
