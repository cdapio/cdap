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

package co.cask.cdap.kafka.run;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.KafkaConstants;
import co.cask.cdap.common.runtime.DaemonMain;
import co.cask.cdap.common.utils.Networks;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.Service;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;

/**
 * Runs embedded Kafka server.
 */
public class KafkaServerMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaServerMain.class);

  private Properties kafkaProperties;
  private EmbeddedKafkaServer kafkaServer;

  public static void main(String [] args) throws Exception {
    new KafkaServerMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    CConfiguration cConf = CConfiguration.create();

    String zkConnectStr = cConf.get(Constants.Zookeeper.QUORUM);
    String zkNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);

    if (zkNamespace != null) {
      ZKClientService client = ZKClientService.Builder.of(zkConnectStr).build();
      try {
        client.startAndWait();

        String path = "/" + zkNamespace;
        LOG.info(String.format("Creating zookeeper namespace %s", path));

        ZKOperations.ignoreError(
          client.create(path, null, CreateMode.PERSISTENT),
          KeeperException.NodeExistsException.class, path).get();

        client.stopAndWait();
        zkConnectStr = String.format("%s/%s", zkConnectStr, zkNamespace);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      } finally {
        client.stopAndWait();
      }
    }

    kafkaProperties = generateKafkaConfig(cConf);

    int partitions = Integer.parseInt(kafkaProperties.getProperty("num.partitions"), 10);
    Preconditions.checkState(partitions > 0, "Num partitions should be greater than zero.");

    int port = Integer.parseInt(kafkaProperties.getProperty("port"), 10);
    Preconditions.checkState(port > 0, "Port number is invalid.");

    String hostname = kafkaProperties.getProperty("host.name");
    InetAddress address = Networks.resolve(hostname, new InetSocketAddress("localhost", 0).getAddress());
    if (hostname != null) {
      if (address.isAnyLocalAddress()) {
        kafkaProperties.remove("host.name");
      } else {
        hostname = address.getCanonicalHostName();
        kafkaProperties.setProperty("host.name", hostname);
      }
    }

    if (kafkaProperties.getProperty("broker.id") == null) {
      int brokerId = generateBrokerId(address);
      LOG.info(String.format("Initializing server with broker id %d", brokerId));
      kafkaProperties.setProperty("broker.id", Integer.toString(brokerId));
    }

    if (kafkaProperties.getProperty("zookeeper.connect") == null) {
      kafkaProperties.setProperty("zookeeper.connect", zkConnectStr);
    }
  }

  @Override
  public void start() {
    LOG.info("Starting embedded kafka server...");

    kafkaServer = new EmbeddedKafkaServer(kafkaProperties);
    Service.State state = kafkaServer.startAndWait();

    if (state != Service.State.RUNNING) {
      throw new  IllegalStateException("Kafka server has not started... terminating.");
    }

    LOG.info("Embedded kafka server started successfully.");
  }

  @Override
  public void stop() {
    LOG.info("Stopping embedded kafka server...");
    if (kafkaServer != null && kafkaServer.isRunning()) {
      kafkaServer.stopAndWait();
    }
  }

  @Override
  public void destroy() {
    // Nothing to do
  }

  private Properties generateKafkaConfig(CConfiguration cConf) {
    Properties prop = new Properties();

    Map<String, String> propConfigs = cConf.getValByRegex("^(kafka\\.server\\.)");
    for (Map.Entry<String, String> pair : propConfigs.entrySet()) {
      String key = pair.getKey();
      String value = cConf.get(key);
      String trimmedKey = key.substring(13);
      prop.setProperty(trimmedKey, value);
    }
    return prop;
  }

  private static int generateBrokerId(InetAddress address) {
    LOG.info("Generating broker ID with address {}", address);
    try {
      return Math.abs(InetAddresses.coerceToInteger(address));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
