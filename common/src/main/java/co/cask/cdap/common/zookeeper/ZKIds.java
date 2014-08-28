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
import org.apache.commons.codec.binary.Base64;
import org.apache.zookeeper.data.Id;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utilities for creating Zookeeper Ids.
 */
public final class ZKIds {

  private ZKIds() { }

  /**
   * Creates a Zookeeper {@link Id} representing a SASL principal.
   *
   * @param principal the SASL principal
   * @return the {@link Id} representing the principal
   */
  public static Id createSasl(String principal) {
    return new Id("sasl", principal);
  }

  /**
   * Creates a Zookeeper {@link Id} representing a username and password identity.
   *
   * @param username username of the {@link Id}
   * @param password password of the {@link Id}
   * @param encoding encoding of the password
   * @return the {@link Id} representing the user
   */
  public static Id createDigest(String username, String password, Charset encoding) {
    try {
      MessageDigest sha1MD = MessageDigest.getInstance("SHA-1");
      String hashedPassword = new String(Base64.encodeBase64(sha1MD.digest(password.getBytes(encoding))), encoding);
      return new Id("digest", username + ":" + hashedPassword);
    } catch (NoSuchAlgorithmException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a Zookeeper {@link Id} representing a username and password identity.
   *
   * @param username username of the {@link Id}
   * @param password UTF-8 password of the {@link Id}
   * @return the {@link Id} representing the username and password
   */
  public static Id createDigest(String username, String password) {
    return createDigest(username, password, Charsets.UTF_8);
  }

  /**
   * Creates a Zookeeper {@link Id} representing a hostname.
   *
   * @param hostname hostname of the {@link Id}
   * @return the {@link Id} representing the hostname
   */
  public static Id createHostname(String hostname) {
    return new Id("host", hostname);
  }

  /**
   * Creates a Zookeeper {@link Id} representing an IP address.
   *
   * @param ip the IP address of the {@link Id}
   * @return the {@link Id} representing the IP address
   */
  public static Id createIp(String ip) {
    return new Id("ip", ip);
  }
}
