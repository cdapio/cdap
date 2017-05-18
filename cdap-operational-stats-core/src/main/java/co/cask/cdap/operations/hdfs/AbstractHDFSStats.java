/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.operations.hdfs;

import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.operations.AbstractOperationalStats;
import co.cask.cdap.operations.OperationalStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.tools.NNHAServiceTarget;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import javax.annotation.Nullable;

/**
 * Base class for {@link OperationalStats} for HDFS
 */
public abstract class AbstractHDFSStats extends AbstractOperationalStats {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHDFSStats.class);
  private static final Logger READ_FAILURE_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(60000));
  @VisibleForTesting
  static final String SERVICE_NAME = "HDFS";

  protected final Configuration conf;

  @VisibleForTesting
  AbstractHDFSStats(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String getServiceName() {
    return SERVICE_NAME;
  }

  /**
   * @return the web URL of the HDFS Namenode, or {@code null} if the web URL cannot be determined.
   */
  @Nullable
  public String getWebURL() {
    try {
      if (HAUtil.isHAEnabled(conf, getNameService())) {
        URL haWebURL = getHAWebURL();
        if (haWebURL != null) {
          return haWebURL.toString();
        }
      } else {
        try (FileSystem fs = FileSystem.get(conf)) {
          URL webUrl = rpcToHttpAddress(fs.getUri());
          if (webUrl != null) {
            return webUrl.toString();
          }
        }
      }
    } catch (Exception e) {
      // TODO: remove once CDAP-7887 is fixed
      READ_FAILURE_LOG.warn("Error in determining HDFS URL. Web URL of HDFS will not be available in HDFS " +
                              "operational stats.", e);
    }
    return null;
  }

  @Nullable
  private String getNameService() {
    Collection<String> nameservices = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
    if (nameservices.isEmpty()) {
      // we want to return null from this method if nameservices are not configured, so it can be used in methods like
      // HAUtil.isHAEnabled()
      return null;
    }
    if (1 == nameservices.size()) {
      return Iterables.getOnlyElement(nameservices);
    }
    throw new IllegalStateException("Found multiple nameservices configured in HDFS. Operational stats currently does" +
                                      " not support HDFS Federation.");
  }

  @Nullable
  private URL getHAWebURL() throws IOException {
    String activeNamenode = null;
    String nameService = getNameService();
    HdfsConfiguration hdfsConf = new HdfsConfiguration(conf);
    String nameNodePrincipal = conf.get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, "");

    hdfsConf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY, nameNodePrincipal);

    for (String nnId : DFSUtilClient.getNameNodeIds(conf, nameService)) {
      HAServiceTarget haServiceTarget = new NNHAServiceTarget(hdfsConf, nameService, nnId);
      HAServiceProtocol proxy = haServiceTarget.getProxy(hdfsConf, 10000);
      HAServiceStatus serviceStatus = proxy.getServiceStatus();
      if (HAServiceProtocol.HAServiceState.ACTIVE != serviceStatus.getState()) {
        continue;
      }
      activeNamenode = DFSUtil.getNamenodeServiceAddr(hdfsConf, nameService, nnId);
    }
    if (activeNamenode == null) {
      throw new IllegalStateException("Could not find an active namenode");
    }
    return rpcToHttpAddress(URI.create(activeNamenode));
  }

  @Nullable
  private URL rpcToHttpAddress(URI rpcURI) throws MalformedURLException {
    String host = rpcURI.getHost();
    if (host == null) {
      return null;
    }
    boolean httpsEnabled = conf.getBoolean(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY, DFSConfigKeys.DFS_HTTPS_ENABLE_DEFAULT);
    String namenodeWebAddress = httpsEnabled ?
      conf.get(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT) :
      conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT);
    InetSocketAddress socketAddress = NetUtils.createSocketAddr(namenodeWebAddress);
    int namenodeWebPort = socketAddress.getPort();
    String protocol = httpsEnabled ? "https" : "http";
    return new URL(protocol, host, namenodeWebPort, "");
  }
}
