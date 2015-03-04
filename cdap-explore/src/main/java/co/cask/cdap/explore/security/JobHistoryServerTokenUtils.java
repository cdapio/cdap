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

import co.cask.cdap.common.security.YarnTokenUtils;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb.GetDelegationTokenRequestPBImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.twill.internal.yarn.YarnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Helper class for getting JobHistoryServer security delegation token.
 */
public final class JobHistoryServerTokenUtils {
  private static final Logger LOG = LoggerFactory.getLogger(YarnTokenUtils.class);

  /**
   * Gets a JHS delegation token and stores it in the given Credentials.
   *
   * @return the same Credentials instance as the one given in parameter.
   */
  public static Credentials obtainToken(Configuration configuration, Credentials credentials) {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return credentials;
    }

    String historyServerAddress = configuration.get("mapreduce.jobhistory.address");
    HostAndPort hostAndPort = HostAndPort.fromString(historyServerAddress);
    try {
      LOG.info("Obtaining delegation token for JHS");

      ResourceMgrDelegate resourceMgrDelegate = new ResourceMgrDelegate(new YarnConfiguration(configuration));
      MRClientCache clientCache = new MRClientCache(configuration, resourceMgrDelegate);
      MRClientProtocol hsProxy = clientCache.getInitializedHSProxy();
      GetDelegationTokenRequest request = new GetDelegationTokenRequestPBImpl();
      request.setRenewer(YarnUtils.getYarnTokenRenewer(configuration));

      InetSocketAddress address = new InetSocketAddress(hostAndPort.getHostText(), hostAndPort.getPort());
      Token<TokenIdentifier> token =
        ConverterUtils.convertFromYarn(hsProxy.getDelegationToken(request).getDelegationToken(), address);

      credentials.addToken(new Text(token.getService()), token);
      return credentials;
    } catch (Exception e) {
      LOG.error("Failed to get secure token for JHS at {}.", hostAndPort, e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Overridden class to expose {@code getInitializedHSProxy}.
   */
  private static class MRClientCache extends ClientCache {

    public MRClientCache(Configuration conf, ResourceMgrDelegate rm) {
      super(conf, rm);
    }

    @Override
    public synchronized MRClientProtocol getInitializedHSProxy() throws IOException {
      return super.getInitializedHSProxy();
    }
  }

  private JobHistoryServerTokenUtils() {
  }
}

