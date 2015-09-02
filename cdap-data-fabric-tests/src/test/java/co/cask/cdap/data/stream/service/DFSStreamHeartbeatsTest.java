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

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.LocationStreamFileWriterFactory;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.service.heartbeat.HeartbeatPublisher;
import co.cask.cdap.data.stream.service.heartbeat.StreamWriterHeartbeat;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.transaction.stream.FileStreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionManager;
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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 *
 */
public class DFSStreamHeartbeatsTest {
  private static final byte[] TWO_BYTES = new byte[] { 'a', 'b' };

  private static String hostname;
  private static int port;

  private static StreamHttpService streamHttpService;
  private static StreamService streamService;
  private static TransactionManager txManager;
  private static DatasetService datasetService;
  private static NotificationService notificationService;
  private static MockHeartbeatPublisher heartbeatPublisher;
  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClient;
  private static StreamAdmin streamAdmin;
  private static NamespacedLocationFactory namespacedLocationFactory;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    cConf.setInt(Constants.Stream.CONTAINER_INSTANCE_ID, 0);

    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(
      Modules.override(
        new ZKClientModule(),
        new DataFabricModules().getInMemoryModules(),
        new ConfigModule(cConf, new Configuration()),
        new DiscoveryRuntimeModule().getInMemoryModules(),
        new LocationRuntimeModule().getInMemoryModules(),
        new ExploreClientModule(),
        new DataSetServiceModules().getInMemoryModules(),
        new DataSetsModules().getStandaloneModules(),
        new NotificationFeedServiceRuntimeModule().getInMemoryModules(),
        new NotificationServiceRuntimeModule().getInMemoryModules(),
        new MetricsClientRuntimeModule().getInMemoryModules(),
        new ViewAdminModules().getInMemoryModules(),
        // We need the distributed modules here to get the distributed stream service, which is the only one
        // that performs heartbeats aggregation
        new StreamServiceRuntimeModule().getDistributedModules(),
        new StreamAdminModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);

          bind(StreamConsumerStateStoreFactory.class).to(LevelDBStreamConsumerStateStoreFactory.class)
            .in(Singleton.class);
          bind(StreamAdmin.class).to(FileStreamAdmin.class).in(Singleton.class);
          bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
          bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);
          bind(StreamFileJanitorService.class).to(LocalStreamFileJanitorService.class).in(Scopes.SINGLETON);

          bind(StreamMetaStore.class).to(InMemoryStreamMetaStore.class).in(Scopes.SINGLETON);
          bind(HeartbeatPublisher.class).to(MockHeartbeatPublisher.class).in(Scopes.SINGLETON);
        }
      }),
      new NamespaceClientRuntimeModule().getDistributedModules());

    zkClient = injector.getInstance(ZKClientService.class);
    txManager = injector.getInstance(TransactionManager.class);
    datasetService = injector.getInstance(DatasetService.class);
    notificationService = injector.getInstance(NotificationService.class);
    streamHttpService = injector.getInstance(StreamHttpService.class);
    streamService = injector.getInstance(StreamService.class);
    heartbeatPublisher = (MockHeartbeatPublisher) injector.getInstance(HeartbeatPublisher.class);
    namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);
    streamAdmin = injector.getInstance(StreamAdmin.class);

    zkClient.startAndWait();
    txManager.startAndWait();
    datasetService.startAndWait();
    notificationService.startAndWait();
    streamHttpService.startAndWait();
    streamService.startAndWait();

    hostname = streamHttpService.getBindAddress().getHostName();
    port = streamHttpService.getBindAddress().getPort();

    Locations.mkdirsIfNotExists(namespacedLocationFactory.get(Id.Namespace.DEFAULT));
  }

  @AfterClass
  public static void afterClass() throws IOException {
    Locations.deleteQuietly(namespacedLocationFactory.get(Id.Namespace.DEFAULT), true);

    notificationService.startAndWait();
    datasetService.startAndWait();
    txManager.startAndWait();
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
    final Id.Stream streamId = Id.Stream.from(Id.Namespace.DEFAULT, streamName);
    // Create a new stream.
    streamAdmin.create(streamId);

    // Enqueue 10 entries
    for (int i = 0; i < entries; ++i) {
      HttpURLConnection urlConn =
        openURL(String.format("http://%s:%d/v3/namespaces/default/streams/%s", hostname, port, streamName),
                HttpMethod.POST);
      urlConn.setDoOutput(true);
      urlConn.addRequestProperty("test_stream1.header1", Integer.toString(i));
      urlConn.getOutputStream().write(TWO_BYTES);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
      urlConn.disconnect();
    }

    Tasks.waitFor((long) entries * TWO_BYTES.length, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        return getStreamSize(streamId, heartbeatPublisher);
      }
    }, Constants.Stream.HEARTBEAT_INTERVAL * 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  private long getStreamSize(Id.Stream streamId, MockHeartbeatPublisher heartbeatPublisher) {
    StreamWriterHeartbeat heartbeat = heartbeatPublisher.getHeartbeat();
    if (heartbeat == null) {
      return 0L;
    }
    Long size = heartbeat.getStreamsSizes().get(streamId);
    return size == null ? 0L : size;
  }

  /**
   * Mock heartbeat publisher that allows to do assertions on the heartbeats being published.
   */
  private static final class MockHeartbeatPublisher extends AbstractIdleService implements HeartbeatPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(MockHeartbeatPublisher.class);
    private final AtomicReference<StreamWriterHeartbeat> heartbeat = new AtomicReference<>();

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
      LOG.info("Received heartbeat {} for Streams {}", heartbeat, heartbeat.getStreamsSizes().keySet());
      this.heartbeat.set(heartbeat);
      return Futures.immediateFuture(heartbeat);
    }

    @Nullable
    public StreamWriterHeartbeat getHeartbeat() {
      return heartbeat.get();
    }
  }
}
