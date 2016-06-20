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

package co.cask.cdap.data.security;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Helper class for getting HBase security delegation token.
 */
public final class HBaseTokenUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTokenUtils.class);

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


// Removed on HBase 2.0:
/*
      commit 48be35cb7effffa0af9e9f0115e643047c4f242a
      Author: Jonathan M Hsieh <jmhsieh@apache.org>
      Date:   Wed Nov 4 15:28:06 2015 -0800

      HBASE-14713 Remove simple deprecated-since-1.0 code in hbase-server from hbase 2.0
*/

      Class c = Class.forName("org.apache.hadoop.hbase.security.token.TokenUtil");
      Method method = c.getMethod("obtainToken", Configuration.class);

      Token<? extends TokenIdentifier> token = castToken(method.invoke(null, hConf));
      credentials.addToken(token.getService(), token);

      return credentials;

    } catch (Exception e) {
      LOG.error("Failed to get secure token for HBase.", e);
      throw Throwables.propagate(e);
    }
  }

  private static <T extends TokenIdentifier> Token<T> castToken(Object obj) {
    return (Token<T>) obj;
  }

  private HBaseTokenUtils() {
  }
}
