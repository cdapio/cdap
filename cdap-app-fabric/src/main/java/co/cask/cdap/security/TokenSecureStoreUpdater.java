/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.security;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.YarnTokenUtils;
import co.cask.cdap.data.security.HBaseTokenUtils;
import co.cask.cdap.hive.ExploreUtils;
import co.cask.cdap.security.hive.HiveTokenUtils;
import co.cask.cdap.security.hive.JobHistoryServerTokenUtils;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStore;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.yarn.YarnUtils;
import org.apache.twill.yarn.YarnSecureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@link SecureStoreUpdater} that updates all secure tokens used by the platform.
 */
public final class TokenSecureStoreUpdater implements SecureStoreUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(TokenSecureStoreUpdater.class);
  private final YarnConfiguration hConf;
  private final LocationFactory locationFactory;
  private final long updateInterval;
  private final boolean secureExplore;

  @Inject
  public TokenSecureStoreUpdater(YarnConfiguration hConf, CConfiguration cConf, LocationFactory locationFactory) {
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    secureExplore = cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED) && UserGroupInformation.isSecurityEnabled();
    updateInterval = calculateUpdateInterval();
  }

  private Credentials refreshCredentials() {
    try {
      Credentials refreshedCredentials = new Credentials();

      if (User.isSecurityEnabled()) {
        YarnTokenUtils.obtainToken(hConf, refreshedCredentials);
      }

      if (User.isHBaseSecurityEnabled(hConf)) {
        HBaseTokenUtils.obtainToken(hConf, refreshedCredentials);
      }

      if (secureExplore) {
        HiveTokenUtils.obtainToken(refreshedCredentials);
        JobHistoryServerTokenUtils.obtainToken(hConf, refreshedCredentials);
      }

      // Perform different logic to get delegation tokens based on the location factory type.
      if (locationFactory instanceof HDFSLocationFactory) {
        YarnUtils.addDelegationTokens(hConf, locationFactory, refreshedCredentials);
      } else if (locationFactory instanceof FileContextLocationFactory) {
        List<Token<?>> tokens = ((FileContextLocationFactory) locationFactory).getFileContext().getDelegationTokens(
          new Path(locationFactory.getHomeLocation().toURI()), YarnUtils.getYarnTokenRenewer(hConf)
        );
        for (Token<?> token : tokens) {
          refreshedCredentials.addToken(token.getService(), token);
        }
      }

      return refreshedCredentials;
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  /**
   * Since Hive classes are not in MasterServiceMain's classpath, create an instance of HiveConf using reflection.
   * Call this method only if explore is enabled.
   */
  private Configuration getHiveConf() {
    ClassLoader hiveClassloader = ExploreUtils.getExploreClassloader();
    ClassLoader contextClassloader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(hiveClassloader);

    try {
      Class<?> clz = hiveClassloader.loadClass("org.apache.hadoop.hive.conf.HiveConf");
      return (Configuration) clz.newInstance();
    } catch (Exception e) {
      LOG.error("Could not create an instance of HiveConf. Using default values.", e);
      return null;
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassloader);
    }
  }

  private long calculateUpdateInterval() {
    List<Long> renewalTimes = Lists.newArrayList();

    renewalTimes.add(hConf.getLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
                                   DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT));

    // The value contains in hbase-default.xml, so it should always there. If it is really missing, default it to 1 day.
    renewalTimes.add(hConf.getLong(Constants.HBase.AUTH_KEY_UPDATE_INTERVAL,
                                   TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));

    if (secureExplore) {
      // Renewal interval for YARN
      renewalTimes.add(hConf.getLong(YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
                                     YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT));

      // Renewal interval for Hive. Also see: https://issues.apache.org/jira/browse/HIVE-9214
      Configuration hiveConf = getHiveConf();
      if (hiveConf != null) {
        renewalTimes.add(hiveConf.getLong(HadoopThriftAuthBridge.Server.DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
                                          HadoopThriftAuthBridge.Server.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT));
      } else {
        renewalTimes.add(HadoopThriftAuthBridge.Server.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
      }

      // Renewal interval for JHS
      renewalTimes.add(hConf.getLong(MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
                                     MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT));
    }

    // Set the update interval to the shortest update interval of all required renewals.
    Long minimumInterval = Collections.min(renewalTimes);
    // Schedule it 5 min before it expires
    long delay = minimumInterval - TimeUnit.MINUTES.toMillis(5);
    // Safeguard: In practice, the value can't be that small, otherwise nothing would work.
    if (delay <= 0) {
      delay = (minimumInterval <= 2) ? 1 : minimumInterval / 2;
    }
    LOG.info("Setting token renewal time to: {} ms", delay);
    return delay;
  }

  /**
   * Returns the minimum update interval for the delegation tokens.
   * @return The update interval in milliseconds.
   */
  public long getUpdateInterval() {
    return updateInterval;
  }

  @Override
  public SecureStore update(String application, RunId runId) {
    Credentials credentials = refreshCredentials();
    LOG.info("Updated credentials {}", credentials.getAllTokens());
    return YarnSecureStore.create(credentials);
  }
}
