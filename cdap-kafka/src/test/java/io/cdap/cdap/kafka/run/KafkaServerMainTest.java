/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.kafka.run;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.KafkaConstants;
import io.cdap.cdap.common.utils.Networks;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.TimeoutException;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link KafkaServerMain}.
 */
public class KafkaServerMainTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testInitStartAndStopWithZkNamespace() throws Exception {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAsync().awaitRunning();
    try {
      int kafkaPort = Networks.getRandomPort();
      String logDir = TEMP_FOLDER.newFolder("kafka-logs").getAbsolutePath();
      String siteXml = "<configuration>"
          + "<property><name>" + Constants.Zookeeper.QUORUM + "</name><value>"
          + zkServer.getConnectionStr() + "</value></property>"
          + "<property><name>" + KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG
          + "</name><value>cdapkafka</value></property>"
          + "<property><name>kafka.server.port</name><value>" + kafkaPort + "</value></property>"
          + "<property><name>kafka.server.host.name</name><value>"
          + InetAddress.getLoopbackAddress().getHostAddress() + "</value></property>"
          + "<property><name>kafka.server.log.dirs</name><value>" + logDir + "</value></property>"
          + "<property><name>kafka.server.num.partitions</name><value>1</value></property>"
          + "<property><name>kafka.server.broker.id</name><value>1</value></property>"
          + "</configuration>";

      KafkaServerMain kafkaMain = new KafkaServerMain();
      runWithSiteXml(siteXml, () -> {
        kafkaMain.init(new String[0]);
        kafkaMain.start();
        try {
          Assert.assertNotNull(kafkaMain);
        } finally {
          kafkaMain.stop();
          kafkaMain.destroy();
        }
      });
    } finally {
      zkServer.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testInitPropagatesZkStartupException() throws Exception {
    int unusedPort = Networks.getRandomPort();
    String siteXml = "<configuration>"
        + "<property><name>" + Constants.Zookeeper.QUORUM + "</name><value>127.0.0.1:"
        + unusedPort + "</value></property>"
        + "<property><name>" + Constants.Zookeeper.CLIENT_STARTUP_TIMEOUT_MILLIS
        + "</name><value>100</value></property>"
        + "<property><name>" + KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG
        + "</name><value>cdapkafka</value></property>"
        + "</configuration>";

    KafkaServerMain kafkaMain = new KafkaServerMain();
    try {
      runWithSiteXml(siteXml, () -> kafkaMain.init(new String[0]));
      Assert.fail("Expected RuntimeException wrapping TimeoutException");
    } catch (RuntimeException actual) {
      Assert.assertTrue(
          "Expected TimeoutException cause, got: " + actual.getCause(),
          actual.getCause() instanceof TimeoutException);
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  private void runWithSiteXml(String siteXml, ThrowingRunnable action) throws Exception {
    File siteFile = new File(TEMP_FOLDER.newFolder(), "cdap-site.xml");
    Files.write(siteFile.toPath(), siteXml.getBytes(StandardCharsets.UTF_8));
    URL fileUrl = siteFile.toURI().toURL();

    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    ClassLoader customLoader = new ClassLoader(previous) {
      @Override
      public URL getResource(String name) {
        if ("cdap-site.xml".equals(name)) {
          return fileUrl;
        }
        return super.getResource(name);
      }

      @Override
      public Enumeration<URL> getResources(String name) throws IOException {
        if ("cdap-site.xml".equals(name)) {
          return Collections.enumeration(Collections.singletonList(fileUrl));
        }
        return super.getResources(name);
      }
    };

    Thread.currentThread().setContextClassLoader(customLoader);
    try {
      action.run();
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }
}
