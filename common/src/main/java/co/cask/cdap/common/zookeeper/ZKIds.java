/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.zookeeper;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import net.iharder.base64.Base64;
import org.apache.zookeeper.data.Id;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utilities for creating Zookeeper Ids.
 */
public class ZKIds {

  public static Id createSaslId(String principal) {
    return new Id("sasl", principal);
  }

  public static Id createDigestId(String username, String password, Charset encoding) {
    try {
      MessageDigest sha1MD = MessageDigest.getInstance("SHA-1");
      String hashedPassword = Base64.encodeBytes(sha1MD.digest(password.getBytes(encoding)));
      return new Id("digest", username + ":" + hashedPassword);
    } catch (NoSuchAlgorithmException e) {
      throw Throwables.propagate(e);
    }
  }

  public static Id createDigestId(String username, String password) {
    return createDigestId(username, password, Charsets.UTF_8);
  }

  public static Id createHostnameId(String hostname) {
    return new Id("host", hostname);
  }

  public static Id createIpId(String ip) {
    return new Id("ip", ip);
  }
}
