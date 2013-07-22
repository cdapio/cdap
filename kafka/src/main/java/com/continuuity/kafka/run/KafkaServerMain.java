package com.continuuity.kafka.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.weave.internal.kafka.EmbeddedKafkaServer;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKOperations;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Properties;

/**
 * Runs embedded Kafka server.
 */
public class KafkaServerMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaServerMain.class);

  private Properties kafkaProperties;
  private EmbeddedKafkaServer kafkaServer;

  private String zkConnectStr;
  private String zkNamespace;

  public static void main(String [] args) throws Exception {
    new KafkaServerMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    int brokerId = generateBrokerId();
    LOG.info(String.format("Initializing server with broker id %d", brokerId));

    CConfiguration cConf = CConfiguration.create();
    zkConnectStr = cConf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);
    zkNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);

    int port = cConf.getInt(KafkaConstants.ConfigKeys.PORT_CONFIG, -1);
    String hostname = cConf.get(KafkaConstants.ConfigKeys.HOSTNAME_CONFIG);
    int numPartitions = cConf.getInt(KafkaConstants.ConfigKeys.NUM_PARTITIONS_CONFIG,
                                     KafkaConstants.DEFAULT_NUM_PARTITIONS);
    String logDir = cConf.get(KafkaConstants.ConfigKeys.LOG_DIR_CONFIG);

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

    kafkaProperties = generateKafkaConfig(brokerId, zkConnectStr, hostname, port, numPartitions, logDir);
  }

  @Override
  public void start() {
    LOG.info("Starting embedded kafka server...");

    kafkaServer = new EmbeddedKafkaServer(KafkaServerMain.class.getClassLoader(), kafkaProperties);
    kafkaServer.startAndWait();

    LOG.info("Embedded kafka server started successfully.");
  }

  @Override
  public void stop() {
    LOG.info("Stopping embedded kafka server...");
    if (kafkaServer != null) {
      kafkaServer.stopAndWait();
    }
  }

  @Override
  public void destroy() {
    // Nothing to do
  }

  private Properties generateKafkaConfig(int brokerId, String zkConnectStr, String hostname, int port,
                                         int numPartitions, String logDir) {
    Preconditions.checkState(port > 0, "Port number is invalid.");
    Preconditions.checkState(numPartitions > 0, "Num partitions should be greater than zero.");

    Properties prop = new Properties();
    prop.setProperty("broker.id", Integer.toString(brokerId));
    if (hostname != null) {
      prop.setProperty("host.name", hostname);
    }
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("log.dir", logDir);
    prop.setProperty("num.partitions", Integer.toString(numPartitions));
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    return prop;
  }

  private static int generateBrokerId() {
    try {
      return Math.abs(InetAddresses.coerceToInteger(InetAddress.getLocalHost()));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
