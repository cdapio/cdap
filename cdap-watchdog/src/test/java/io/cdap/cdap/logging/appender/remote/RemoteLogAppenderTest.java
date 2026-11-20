/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.remote;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RemoteLogAppender} MD5 partitioning and start/stop lifecycle.
 */
public class RemoteLogAppenderTest {

  @Test
  public void testDeterministicPartitionMethodForEightAndSixteenPartitions() throws Exception {
    Method partitionMethod =
        RemoteLogAppender.class.getDeclaredMethod("partition", String.class, int.class);
    partitionMethod.setAccessible(true);

    Assert.assertEquals(1, partitionMethod.invoke(null, "default:myApp:worker:myWorker", 8));
    Assert.assertEquals(9, partitionMethod.invoke(null, "default:myApp:worker:myWorker", 16));

    Assert.assertEquals(1, partitionMethod.invoke(null, "system:services:appfabric", 8));
    Assert.assertEquals(1, partitionMethod.invoke(null, "system:services:appfabric", 16));

    Assert.assertEquals(3, partitionMethod.invoke(null, "prod_ns:etl_pipeline:spark:driver", 8));
    Assert.assertEquals(11, partitionMethod.invoke(null, "prod_ns:etl_pipeline:spark:driver", 16));

    Assert.assertEquals(0, partitionMethod.invoke(null, "ns_üñîçødé:app:worker:w1", 8));
    Assert.assertEquals(8, partitionMethod.invoke(null, "ns_üñîçødé:app:worker:w1", 16));

    Assert.assertEquals(7, partitionMethod.invoke(null, "ns_日本語:app_λ:mapreduce:mr1", 8));
    Assert.assertEquals(15, partitionMethod.invoke(null, "ns_日本語:app_λ:mapreduce:mr1", 16));

    Assert.assertEquals(3, partitionMethod.invoke(null, "ns_🚀:app_é:workflow:wf1", 8));
    Assert.assertEquals(11, partitionMethod.invoke(null, "ns_🚀:app_é:workflow:wf1", 16));
  }

  @Test
  public void testLifecycleAndPublishedPartitionRouting() throws Exception {
    RecordingPublishHandler handler = new RecordingPublishHandler();
    NettyHttpService httpService = NettyHttpService.builder("remote-log-test")
        .setHost("127.0.0.1")
        .setPort(0)
        .setHttpHandlers(handler)
        .build();
    httpService.start();
    try {
      InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
      discoveryService.register(
          new Discoverable(Constants.Service.LOGSAVER, httpService.getBindAddress()));
      RemoteClientFactory clientFactory = new RemoteClientFactory(
          discoveryService, new DefaultInternalAuthenticator(new AuthenticationTestContext()));

      // Verify 8-partition routing and start -> restart -> stop lifecycle
      CConfiguration conf8 = CConfiguration.create();
      conf8.setInt(Constants.Logging.NUM_PARTITIONS, 8);
      conf8.set(Constants.Logging.LOG_PUBLISH_PARTITION_KEY, "program");

      RemoteLogAppender appender8 = new RemoteLogAppender(conf8, clientFactory);
      appender8.setContext(new LoggerContext());
      appender8.start();
      appender8.append(createLogMessage("default", "myApp", "worker:myWorker", "warmup-8"));
      // Restarting exercises Optional.ofNullable(...).ifPresent(...) stopping previous publisher
      appender8.start();
      Assert.assertTrue(appender8.isStarted());

      appender8.append(createLogMessage("ns_üñîçødé", "app", "worker:w1", "unicode-8"));
      appender8.stop();
      appender8.stop();
      Assert.assertFalse(appender8.isStarted());
      Assert.assertTrue(handler.partitions.contains(0));

      // Verify 16-partition routing for the same Unicode key (maps to partition 8)
      handler.partitions.clear();
      CConfiguration conf16 = CConfiguration.create();
      conf16.setInt(Constants.Logging.NUM_PARTITIONS, 16);
      conf16.set(Constants.Logging.LOG_PUBLISH_PARTITION_KEY, "program");

      RemoteLogAppender appender16 = new RemoteLogAppender(conf16, clientFactory);
      appender16.setContext(new LoggerContext());
      appender16.start();
      appender16.append(createLogMessage("ns_üñîçødé", "app", "worker:w1", "unicode-16"));
      appender16.append(createLogMessage("default", "myApp", "worker:myWorker", "ascii-16"));
      appender16.stop();

      Assert.assertTrue(handler.partitions.contains(8));
      Assert.assertTrue(handler.partitions.contains(9));
    } finally {
      httpService.stop();
    }
  }

  private static LogMessage createLogMessage(String ns, String app, String worker, String msg) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(Level.INFO);
    event.setLoggerName("io.cdap.RemoteTest");
    event.setMessage(msg);
    event.setTimeStamp(1700000000000L);
    event.setCallerData(new StackTraceElement[0]);
    return new LogMessage(event, new WorkerLoggingContext(ns, app, worker, "run1", "0"));
  }

  /**
   * Minimal Netty HTTP handler recording partition IDs posted by {@link RemoteLogAppender}.
   */
  public static final class RecordingPublishHandler extends AbstractHttpHandler {

    private final List<Integer> partitions = new CopyOnWriteArrayList<>();

    @POST
    @Path("/v1/logs/partitions/{partition-id}/publish")
    public void publish(FullHttpRequest request, HttpResponder responder,
        @PathParam("partition-id") int partitionId) throws InterruptedException {
      if (request.content().isReadable()) {
        partitions.add(partitionId);
        java.util.concurrent.TimeUnit.MILLISECONDS.sleep(40);
      }
      responder.sendStatus(HttpResponseStatus.OK);
    }
  }
}
