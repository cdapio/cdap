package com.continuuity.explore.security;

import com.continuuity.explore.service.ExploreServiceUtils;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * Helper class to get Hive delegation token.
 */
public class HiveTokenUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTokenUtils.class);

  public static Credentials obtainToken(Credentials credentials) {
    ClassLoader hiveClassloader = ExploreServiceUtils.getExploreClassLoader();
    try {
      Class hiveConfClass = hiveClassloader.loadClass("org.apache.hadoop.hive.conf.HiveConf");
      Object hiveConf = hiveConfClass.newInstance();

      Class hiveClass = hiveClassloader.loadClass("org.apache.hadoop.hive.ql.metadata.Hive");
      @SuppressWarnings("unchecked")
      Method hiveGet = hiveClass.getMethod("get", hiveConfClass);
      Object hiveObject = hiveGet.invoke(null, hiveConf);

      String user = UserGroupInformation.getCurrentUser().getShortUserName();
      LOG.info("Fetching Hive MetaStore delegation token for user {}", user);

      @SuppressWarnings("unchecked")
      Method getDelegationToken = hiveClass.getMethod("getDelegationToken", String.class, String.class);
      String tokenStr = (String) getDelegationToken.invoke(hiveObject, user, user);

      Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>();
      delegationToken.decodeFromUrlString(tokenStr);
      LOG.info("Adding delegation token from MetaStore for service {} for user{}", delegationToken.getService(), user);
      credentials.addToken(delegationToken.getService(), delegationToken);
      return credentials;
    } catch (Exception e) {
      LOG.error("Got exception when fetching delegation token from Hive MetaStore", e);
      throw Throwables.propagate(e);
    }
  }
}
