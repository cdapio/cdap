package com.continuuity.kafka.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.weave.internal.kafka.EmbeddedKafkaServer;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

/**
 * Runs embedded Kafka server.
 */
public class KafkaServerMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaServerMain.class);

  private static final String KAFKA_CONFIG = "kafka.server.config";
  private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";

  private CConfiguration cConf;
  private EmbeddedKafkaServer kafkaServer;

  @Override
  public void init(String[] args) {
    cConf = CConfiguration.create();
  }

  @Override
  public void start() {
    LOG.info("Starting embedded kafka server...");

    String configFile = cConf.get(KAFKA_CONFIG);
    LOG.info(String.format("Using config file %s", configFile));
    Properties kafkaConfig = readKafkaConfig(configFile);

    // If zookeeper config is not present, then read it from CConfiguration
    if (kafkaConfig.getProperty(ZOOKEEPER_CONNECT) == null) {
      kafkaConfig.setProperty(ZOOKEEPER_CONNECT, cConf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE));
    }

    kafkaServer = new EmbeddedKafkaServer(KafkaServerMain.class.getClassLoader(), kafkaConfig);
    kafkaServer.startAndWait();

    LOG.info("Embedded kafka server started successfully.");
  }

  @Override
  public void stop() {
    LOG.info("Stopping embedded kafka server...");
    kafkaServer.stopAndWait();
  }

  @Override
  public void destroy() {
    // Nothing to do
  }

  private Properties readKafkaConfig(String configFile) {
    Reader reader = null;
    try {
      reader = new FileReader(configFile);
      Properties properties = new Properties();
      properties.load(reader);
      return properties;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error(String.format("Exception while closing reader of file %s", configFile), e);
        }
      }
    }
  }
}
