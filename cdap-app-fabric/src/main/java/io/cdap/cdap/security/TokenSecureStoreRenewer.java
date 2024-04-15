/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.security;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.DelegationTokensUpdater;
import io.cdap.cdap.common.security.YarnTokenUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.api.security.SecureStoreWriter;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.yarn.YarnUtils;
import org.apache.twill.yarn.YarnSecureStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SecureStoreRenewer} implementation that renew delegation tokens for YARN applications
 * that are launched by CDAP.
 */
public class TokenSecureStoreRenewer extends SecureStoreRenewer {

  private static final Logger LOG = LoggerFactory.getLogger(TokenSecureStoreRenewer.class);

  private final YarnConfiguration yarnConf;
  private final CConfiguration cConf;
  private final LocationFactory locationFactory;
  private final SecureStore secureStore;
  private Long updateInterval;

  @Inject
  TokenSecureStoreRenewer(YarnConfiguration yarnConf, CConfiguration cConf,
      LocationFactory locationFactory,
      SecureStore secureStore) {
    this.yarnConf = yarnConf;
    this.cConf = cConf;
    this.locationFactory = locationFactory;
    this.secureStore = secureStore;
  }

  /**
   * Returns the minimum update interval for the delegation tokens.
   *
   * @return The update interval in milliseconds.
   */
  public long getUpdateInterval() {
    if (updateInterval == null) {
      // we want to lazily call this (as opposed to in the constructor), because sometimes we use an instance of
      // TokenSecureStoreRenewer without scheduling updates. For instance, when launching a program or from
      // ImpersonationHandler.
      updateInterval = calculateUpdateInterval();
    }
    return updateInterval;
  }

  @Override
  public void renew(String application, RunId runId, SecureStoreWriter secureStoreWriter)
      throws IOException {
    Credentials credentials = createCredentials();
    UserGroupInformation currentUser = null;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      // this shouldn't happen
      LOG.debug("Cannot determine current user", e);
    }
    LOG.debug("Updating credentials for application {}, run {}, tokens {}, with current user {}",
        application, runId, credentials.getAllTokens(), currentUser);
    secureStoreWriter.write(YarnSecureStore.create(credentials));
  }

  /**
   * Creates a {@link Credentials} that contains delegation tokens of the current user for all
   * services that CDAP uses.
   */
  public Credentials createCredentials() {
    Credentials refreshedCredentials = new Credentials();

    if (UserGroupInformation.isSecurityEnabled()) {
      YarnTokenUtils.obtainToken(yarnConf, refreshedCredentials);
    }

    try {
      if (secureStore instanceof DelegationTokensUpdater) {
        String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
        ((DelegationTokensUpdater) secureStore).addDelegationTokens(renewer, refreshedCredentials);
      }

      YarnUtils.addDelegationTokens(yarnConf, locationFactory, refreshedCredentials);
    } catch (IOException e) {
      LOG.warn("Failed to refresh Hadoop delegation tokens", e);
    }

    return refreshedCredentials;
  }

  private long calculateUpdateInterval() {
    return calculateUpdateInterval(yarnConf);
  }

  /**
   * Calculates the secure token update interval based on the given configurations.
   *
   * @param hConf the YARN configuration
   * @return time in millisecond that the secure token should be updated
   */
  public static long calculateUpdateInterval(Configuration hConf) {

    return calculateUpdateIntervalHelper(hConf);
  }

  private static long calculateUpdateIntervalHelper(Configuration hConf) {
    List<Long> renewalTimes = Lists.newArrayList();

    renewalTimes.add(hConf.getLong(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT));

    // The value contains in hbase-default.xml, so it should always there. If it is really missing, default it to 1 day.
    renewalTimes.add(hConf.getLong(Constants.HBase.AUTH_KEY_UPDATE_INTERVAL,
        TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));

    // Set the update interval to the shortest update interval of all required renewals.
    Long minimumInterval = Collections.min(renewalTimes);
    // Schedule it 1 hour before it expires
    long delay = minimumInterval - TimeUnit.HOURS.toMillis(1);
    // Safeguard: In practice, the value can't be that small, otherwise nothing would work.
    if (delay <= 0) {
      delay = (minimumInterval <= 2) ? 1 : minimumInterval / 2;
    }
    LOG.info("Setting token renewal time to: {} ms", delay);
    return delay;
  }
}
