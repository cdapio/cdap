package com.continuuity.data.security;

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
