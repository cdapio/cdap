/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.explore.security;

import co.cask.cdap.explore.service.ExploreServiceUtils;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Helper class for getting Hive security delegation token.
 */
public final class HiveTokenUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTokenUtils.class);

  public static Credentials obtainToken(Credentials credentials) {
    ClassLoader hiveClassloader = ExploreServiceUtils.getExploreClassLoader();
    ClassLoader contextClassloader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(hiveClassloader);

    try {
      LOG.info("Obtaining delegation token for Hive");
      Class hiveConfClass = hiveClassloader.loadClass("org.apache.hadoop.hive.conf.HiveConf");
      Object hiveConf = hiveConfClass.newInstance();

      Class hiveClass = hiveClassloader.loadClass("org.apache.hadoop.hive.ql.metadata.Hive");
      @SuppressWarnings("unchecked")
      Method hiveGet = hiveClass.getMethod("get", hiveConfClass);
      Object hiveObject = hiveGet.invoke(null, hiveConf);

      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      @SuppressWarnings("unchecked")
      Method getDelegationToken = hiveClass.getMethod("getDelegationToken", String.class, String.class);
      String tokenStr = (String) getDelegationToken.invoke(hiveObject, user, user);

      Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>();
      delegationToken.decodeFromUrlString(tokenStr);
      delegationToken.setService(new Text(HiveAuthFactory.HS2_CLIENT_TOKEN));
      LOG.info("Adding delegation token {} from MetaStore for service {} for user {}",
               delegationToken, delegationToken.getService(), user);
      credentials.addToken(delegationToken.getService(), delegationToken);
      return credentials;
    } catch (Exception e) {
      LOG.error("Exception when fetching delegation token from Hive MetaStore", e);
      throw Throwables.propagate(e);
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassloader);
    }
  }

  private HiveTokenUtils() {
  }
}
