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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.LocationStreamFileWriterFactory;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.heartbeat.HeartbeatPublisher;
import co.cask.cdap.data.stream.service.heartbeat.StreamWriterHeartbeat;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileAdmin;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DFSStreamHeartbeatsTest {
  private static final byte[] TWO_BYTES = new byte[] { 'a', 'b' };

  private static String hostname;
  private static int port;
  private static CConfiguration conf;

  private static Injector injector;
  private static StreamHttpService streamHttpService;
  private static StreamService streamService;
  private static MockHeartbeatPublisher heartbeatPublisher;
  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClient;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    conf = CConfiguration.create();
    conf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    conf.setInt(Constants.Stream.CONTAINER_INSTANCE_ID, 0);

    conf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    injector = Guice.createInjector(
      Modules.override(
        new ZKClientModule(),
        new DataFabricModules().getInMemoryModules(),
        new ConfigModule(conf, new Configuration()),
        new AuthModule(),
        new DiscoveryRuntimeModule().getInMemoryModules(),
        new LocationRuntimeModule().getInMemoryModules(),
        new ExploreClientModule(),
        new DataSetServiceModules().getInMemoryModule(),
        new DataSetsModules().getLocalModule(),
        new NotificationFeedServiceRuntimeModule().getInMemoryModules(),
        new NotificationServiceRuntimeModule().getInMemoryModules(),
        // We need the distributed modules here to get the distributed stream service, which is the only one
        // that performs heartbeats aggregation
        new StreamServiceRuntimeModule().getDistributedModules(),
        new StreamAdminModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);

          bind(StreamConsumerStateStoreFactory.class).to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
          bind(StreamAdmin.class).to(LevelDBStreamFileAdmin.class).in(Singleton.class);
          bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
          bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);
          bind(StreamFileJanitorService.class).to(LocalStreamFileJanitorService.class).in(Scopes.SINGLETON);

          bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class).in(Scopes.SINGLETON);
          bind(HeartbeatPublisher.class).to(MockHeartbeatPublisher.class).in(Scopes.SINGLETON);
        }
      }));

    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    streamHttpService = injector.getInstance(StreamHttpService.class);
    streamHttpService.startAndWait();
    streamService = injector.getInstance(StreamService.class);
    streamService.startAndWait();
    heartbeatPublisher = (MockHeartbeatPublisher) injector.getInstance(HeartbeatPublisher.class);

    hostname = streamHttpService.getBindAddress().getHostName();
    port = streamHttpService.getBindAddress().getPort();
  }

  @AfterClass
  public static void afterClass() {
    streamService.stopAndWait();
    streamHttpService.stopAndWait();
    zkClient.stopAndWait();
    zkServer.stopAndWait();
  }

  private HttpURLConnection openURL(String location, HttpMethod method) throws IOException {
    URL url = new URL(location);
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setRequestMethod(method.getName());
    return urlConn;
  }

  @Test
  public void streamPublishesHeartbeatTest() throws Exception {
    final int entries = 10;
    final String streamName = "test_stream";

    // Create a new stream.
    HttpURLConnection urlConn = openURL(String.format("http://%s:%d/v2/streams/%s", hostname, port, streamName),
                                        HttpMethod.PUT);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // Enqueue 10 entries
    for (int i = 0; i < entries; ++i) {
      urlConn = openURL(String.format("http://%s:%d/v2/streams/%s", hostname, port, streamName), HttpMethod.POST);
      urlConn.setDoOutput(true);
      urlConn.addRequestProperty("test_stream1.header1", Integer.toString(i));
      urlConn.getOutputStream().write(TWO_BYTES);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
      urlConn.disconnect();
    }

    TimeUnit.SECONDS.sleep(Constants.Stream.HEARTBEAT_INTERVAL + 1);
    StreamWriterHeartbeat heartbeat = heartbeatPublisher.getHeartbeat();
    Assert.assertNotNull(heartbeat);
    Assert.assertEquals(1, heartbeat.getStreamsSizes().size());
    Long streamSize = heartbeat.getStreamsSizes().get(streamName);
    Assert.assertNotNull(streamSize);
    Assert.assertEquals(entries * TWO_BYTES.length, (long) streamSize);
  }

  /**
   * Mock heartbeat publisher that allows to do assertions on the heartbeats being published.
   */
  private static final class MockHeartbeatPublisher extends AbstractIdleService implements HeartbeatPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(MockHeartbeatPublisher.class);
    private StreamWriterHeartbeat heartbeat = null;

    @Override
    protected void startUp() throws Exception {
      LOG.info("Starting Publisher.");
    }

    @Override
    protected void shutDown() throws Exception {
      LOG.info("Stopping Publisher.");
    }

    @Override
    public ListenableFuture<StreamWriterHeartbeat> sendHeartbeat(StreamWriterHeartbeat heartbeat) {
      LOG.info("Received heartbeat {} for Stream {}", heartbeat);
      this.heartbeat = heartbeat;
      return Futures.immediateFuture(heartbeat);
    }

    public StreamWriterHeartbeat getHeartbeat() {
      return heartbeat;
    }
  }
}
