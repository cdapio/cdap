/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.data.security;

import com.google.common.base.Throwables;
import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Helper class for getting HBase security delegation token.
 */
public final class HBaseTokenUtils {

  /**
   * Gets a HBase delegation token and stores it in the given Credentials.
   *
   * @return the same Credentials instance as the one given in parameter.
   */
  public static Credentials obtainToken(Configuration hConf, Credentials credentials) {
    if (!User.isHBaseSecurityEnabled(hConf)) {
      return credentials;
    }

    try {
      Class c = Class.forName("org.apache.hadoop.hbase.security.token.TokenUtil");
      Method method = c.getMethod("obtainToken", Configuration.class);

      Token<? extends TokenIdentifier> token = castToken(method.invoke(null, hConf));
      credentials.addToken(token.getService(), token);

      return credentials;

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static <T extends TokenIdentifier> Token<T> castToken(Object obj) {
    return (Token<T>) obj;
  }

  private HBaseTokenUtils() {
  }
}
