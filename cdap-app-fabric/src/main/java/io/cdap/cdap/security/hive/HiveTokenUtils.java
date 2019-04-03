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

package co.cask.cdap.security.hive;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.hive.ExploreUtils;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Helper class for getting Hive security delegation token.
 */
public final class HiveTokenUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTokenUtils.class);
  private static final Properties EMPTY_PROPERTIES = new Properties();

  public static void obtainTokens(CConfiguration cConf, Credentials credentials) {
    ClassLoader hiveClassloader = ExploreUtils.getExploreClassloader();
    ClassLoader contextClassloader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(hiveClassloader);

    try {
      obtainHiveMetastoreToken(hiveClassloader, credentials);
      obtainHiveServer2Token(hiveClassloader, cConf, credentials);
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassloader);
    }
  }

  private static void obtainHiveMetastoreToken(ClassLoader hiveClassloader, Credentials credentials) {
    try {
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

      Token<DelegationTokenIdentifier> delegationToken = new Token<>();
      delegationToken.decodeFromUrlString(tokenStr);
      delegationToken.setService(new Text(Constants.Explore.HIVE_METASTORE_TOKEN_SERVICE_NAME));
      LOG.debug("Adding delegation token {} from MetaStore for service {} for user {}",
                delegationToken, delegationToken.getService(), user);
      credentials.addToken(delegationToken.getService(), delegationToken);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static void obtainHiveServer2Token(ClassLoader hiveClassloader, CConfiguration cConf,
                                             Credentials credentials) {
    String hiveJdbcUrl = cConf.get(Constants.Explore.HIVE_SERVER_JDBC_URL);
    // if Hive JDBC URL is not set, then we don't obtain the delegation tokens for HiveServer2.
    // This token is required for connecting to HiveService2 from user containers, so it is not strictly
    // required to be present for CDAP functionality.
    if (Strings.isNullOrEmpty(hiveJdbcUrl)) {
      LOG.debug("Hive JDBC URL is not set, not fetching delegation token from HiveServer2");
      return;
    }

    try {
      Class hiveConnectionClass = hiveClassloader.loadClass("org.apache.hive.jdbc.HiveConnection");
      @SuppressWarnings("unchecked")
      Constructor constructor = hiveConnectionClass.getConstructor(String.class, Properties.class);
      @SuppressWarnings("unchecked")
      Method closeMethod = hiveConnectionClass.getMethod("close");
      @SuppressWarnings("unchecked")
      Method getDelegationTokenMethod =
        hiveConnectionClass.getMethod("getDelegationToken", String.class, String.class);

      Object hiveConnection = constructor.newInstance(hiveJdbcUrl, EMPTY_PROPERTIES);

      try {
        String user = UserGroupInformation.getCurrentUser().getShortUserName();
        String tokenStr = (String) getDelegationTokenMethod.invoke(hiveConnection, user, user);
        Token<DelegationTokenIdentifier> delegationToken = new Token<>();
        delegationToken.decodeFromUrlString(tokenStr);
        LOG.debug("Adding delegation token {} from HiveServer2 for service {} for user {}",
                  delegationToken, delegationToken.getService(), user);
        credentials.addToken(delegationToken.getService(), delegationToken);
      } finally {
        closeMethod.invoke(hiveConnection);
      }
    } catch (Exception e) {
      LOG.warn("Got exception when fetching delegation token from HiveServer2 using JDBC URL {}, ignoring it",
               hiveJdbcUrl, e);
    }
  }

  private HiveTokenUtils() {
  }
}
