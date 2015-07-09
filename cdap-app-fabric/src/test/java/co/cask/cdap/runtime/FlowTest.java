/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.runtime;

import co.cask.cdap.ArgumentCheckApp;
import co.cask.cdap.InvalidFlowOutputApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.stream.StreamEventCodec;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.runtime.app.PendingMetricTestApp;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(SlowTests.class)
public class FlowTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Supplier<File> TEMP_FOLDER_SUPPLIER = new Supplier<File>() {

    @Override
    public File get() {
      try {
        return tmpFolder.newFolder();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  };

  private static final Logger LOG = LoggerFactory.getLogger(FlowTest.class);

  private static MetricStore metricStore;


  @BeforeClass
  public static void init() throws AlreadyExistsException, NamespaceCannotBeCreatedException {
    NamespaceAdmin namespaceAdmin = AppFabricTestHelper.getInjector().getInstance(NamespaceAdmin.class);
    namespaceAdmin.createNamespace(Constants.DEFAULT_NAMESPACE_META);
    metricStore = AppFabricTestHelper.getInjector().getInstance(MetricStore.class);
  }

  @Test
  public void testAppWithArgs() throws Exception {
   final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(ArgumentCheckApp.class,
                                                                                        TEMP_FOLDER_SUPPLIER);
   ProgramRunnerFactory runnerFactory = AppFabricTestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    // Only running flow is good. But, in case of service, we need to send something to service as it's lazy loading
    List<ProgramController> controllers = Lists.newArrayList();
    for (final Program program : app.getPrograms()) {
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      BasicArguments systemArgs = new BasicArguments(ImmutableMap.of(ProgramOptionConstants.RUN_ID,
                                                                     RunIds.generate().getId()));
      BasicArguments userArgs = new BasicArguments(ImmutableMap.of("arg", "test"));

      controllers.add(runner.run(program, new SimpleProgramOptions(program.getName(), systemArgs, userArgs)));
    }

    TimeUnit.SECONDS.sleep(1);
    DiscoveryServiceClient discoveryServiceClient = AppFabricTestHelper.getInjector().
                                                    getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = discoveryServiceClient.discover(
      String.format("service.%s.%s.%s",
                    DefaultId.NAMESPACE.getId(), "ArgumentCheckApp", "SimpleService")).iterator().next();

    URL url = new URL(String.format("http://%s:%d/v3/namespaces/default/apps/%s/services/%s/methods/%s",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    "ArgumentCheckApp",
                                    "SimpleService",
                                    "ping"));
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    // this would fail had the service been started without the argument (initialize would have thrown)
    Assert.assertEquals(200, urlConn.getResponseCode());

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }

  @Test
  public void testFlow() throws Exception {
    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(WordCountApp.class,
                                                                                         TEMP_FOLDER_SUPPLIER);
    ProgramRunnerFactory runnerFactory = AppFabricTestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    List<ProgramController> controllers = Lists.newArrayList();

    for (final Program program : app.getPrograms()) {
      // running mapreduce is out of scope of this tests (there's separate unit-test for that)
      if (program.getType() == ProgramType.MAPREDUCE) {
        continue;
      }
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      BasicArguments systemArgs = new BasicArguments(ImmutableMap.of(ProgramOptionConstants.RUN_ID,
                                                                     RunIds.generate().getId()));
      controllers.add(runner.run(program, new SimpleProgramOptions(program.getName(), systemArgs,
                                                                   new BasicArguments())));
    }

    TimeUnit.SECONDS.sleep(1);

    TransactionSystemClient txSystemClient = AppFabricTestHelper.getInjector().
                                             getInstance(TransactionSystemClient.class);

    QueueName queueName = QueueName.fromStream(app.getId().getNamespaceId(), "text");
    QueueClientFactory queueClientFactory = AppFabricTestHelper.getInjector().getInstance(QueueClientFactory.class);
    QueueProducer producer = queueClientFactory.createProducer(queueName);

    // start tx to write in queue in tx
    Transaction tx = txSystemClient.startShort();
    ((TransactionAware) producer).startTx(tx);

    StreamEventCodec codec = new StreamEventCodec();
    for (int i = 0; i < 10; i++) {
      String msg = "Testing message " + i;
      StreamEvent event = new StreamEvent(ImmutableMap.<String, String>of(),
                                          ByteBuffer.wrap(msg.getBytes(Charsets.UTF_8)));
      producer.enqueue(new QueueEntry(codec.encodePayload(event)));
    }

    // commit tx
    ((TransactionAware) producer).commitTx();
    txSystemClient.commit(tx);

    // Query the service for at most 10 seconds for the expected result
    Gson gson = new Gson();
    DiscoveryServiceClient discoveryServiceClient = AppFabricTestHelper.getInjector().
      getInstance(DiscoveryServiceClient.class);
    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover(
      String.format("service.%s.%s.%s", DefaultId.NAMESPACE.getId(), "WordCountApp", "WordFrequencyService"));
    EndpointStrategy endpointStrategy = new RandomEndpointStrategy(serviceDiscovered);
    int trials = 0;
    while (trials++ < 10) {
      Discoverable discoverable = endpointStrategy.pick(2, TimeUnit.SECONDS);
      URL url = new URL(String.format("http://%s:%d/v3/namespaces/default/apps/%s/services/%s/methods/%s/%s",
                                      discoverable.getSocketAddress().getHostName(),
                                      discoverable.getSocketAddress().getPort(),
                                      "WordCountApp",
                                      "WordFrequencyService",
                                      "wordfreq",
                                      "text:Testing"));
      try {
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        Map<String, Long> responseContent = gson.fromJson(
          new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8),
          new TypeToken<Map<String, Long>>() { }.getType());

        LOG.info("Service response: " + responseContent);
        if (ImmutableMap.of("text:Testing", 10L).equals(responseContent)) {
          break;
        }

      } catch (Throwable t) {
        LOG.info("Exception when trying to query service.", t);
      }

      TimeUnit.SECONDS.sleep(1);
    }

    Assert.assertTrue(trials < 10);

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testInvalidOutputEmitter() throws Throwable {
    try {
      AppFabricTestHelper.deployApplicationWithManager(InvalidFlowOutputApp.class, TEMP_FOLDER_SUPPLIER);
    } catch (Exception e) {
      throw Throwables.getRootCause(e);
    }
  }

  @Test
  public void testFlowPendingMetric() throws Exception {

    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(
      PendingMetricTestApp.class, TEMP_FOLDER_SUPPLIER);
    ProgramRunnerFactory runnerFactory = AppFabricTestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    File tempFolder = TEMP_FOLDER_SUPPLIER.get();

    ProgramController controller = null;
    for (final Program program : app.getPrograms()) {
      // running mapreduce is out of scope of this tests (there's separate unit-test for that)
      if (program.getType() == ProgramType.FLOW) {
        ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
        BasicArguments systemArgs = new BasicArguments(ImmutableMap.of(ProgramOptionConstants.RUN_ID,
                                                                       RunIds.generate().getId()));
        controller = runner.run(program, new SimpleProgramOptions(
          program.getName(), systemArgs, new BasicArguments(ImmutableMap.of("temp", tempFolder.getAbsolutePath(),
                                                                            "count", "4"))));
      }
    }
    Assert.assertNotNull(controller);
    Map<String, String> tagsForSourceToOne = metricTagsForQueue("source", "ints", "forward-one");
    Map<String, String> tagsForSourceToTwo = metricTagsForQueue("source", null, "forward-two");
    Map<String, String> tagsForSourceToTwoInts = metricTagsForQueue("source", "ints", "forward-two");
    Map<String, String> tagsForSourceToTwoStrings = metricTagsForQueue("source", "strings", "forward-two");
    Map<String, String> tagsForOneToSink = metricTagsForQueue("forward-one", "queue", "sink");
    Map<String, String> tagsForTwoToSink = metricTagsForQueue("forward-two", "queue", "sink");
    Map<String, String> tagsForAllToOne = metricTagsForQueue(null, null, "forward-one");
    Map<String, String> tagsForAllToTwo = metricTagsForQueue(null, null, "forward-two");
    Map<String, String> tagsForAllToSink = metricTagsForQueue(null, null, "sink");
    Map<String, String> tagsForAll = metricTagsForQueue(null, null, null);

    // each flowlets is waiting for a file to appear in the temp folder. Until then it does not process any event
    // however, the flowlet driver emits metrics before it calls prcess(), hence it wil always seem as if one event
    // is already processed (not pending). We will kick off the flowlets one by one to validate the pending metrics.

    try {
      // source emits 4, then forward-one reads 1, hence 3 should be pending
      waitForPending(tagsForSourceToOne, 3, 5000); // wait a little longer as flow needs to start
      waitForPending(tagsForAllToOne, 3, 100); // wait a little longer as flow needs to start
      // forward-two receives each of the 4 as a sting and an int, but has read only 1
      // so there should be 3 + 4 = 7 pending, but we don't know which queue has 3 and which has 4
      long intPending = waitForPending(tagsForSourceToTwoInts, 3, 4L, 1000);
      waitForPending(tagsForSourceToTwoStrings, 7 - intPending, 1000);
      waitForPending(tagsForSourceToTwo, 7, 100);
      waitForPending(tagsForAllToTwo, 7, 100);
      // neither one nor two have emitted, so the total pending should be 9
      waitForPending(tagsForAll, 10, 100);

      // kick on forward-one, it should now consume all its events
      Assert.assertTrue(new File(tempFolder, "one").createNewFile());
      waitForPending(tagsForSourceToOne, 0, 2000);
      waitForPending(tagsForAllToOne, 0, 100);
      // sink has received 4 but started to read 1, so it has 3 pending
      waitForPending(tagsForOneToSink, 3, 1000);
      waitForPending(tagsForAllToSink, 3, 100);

      // kick-off forward-two, it should now consume all its integer and string events
      Assert.assertTrue(new File(tempFolder, "two-i").createNewFile());
      Assert.assertTrue(new File(tempFolder, "two-s").createNewFile());
      // pending events for all of forward-two's queues should go to zero
      waitForPending(tagsForSourceToTwoInts, 0, 2000);
      waitForPending(tagsForSourceToTwoStrings, 0, 1000);
      waitForPending(tagsForSourceToTwo, 0, 1000);
      waitForPending(tagsForAllToTwo, 0, 100);
      // but now sink should have 8 more events waiting
      waitForPending(tagsForOneToSink, 3, 1000);
      waitForPending(tagsForTwoToSink, 8, 1000);
      waitForPending(tagsForAllToSink, 11, 100);

      // kick off sink, its penidng events should now go to zero
      Assert.assertTrue(new File(tempFolder, "three").createNewFile());
      waitForPending(tagsForOneToSink, 0, 2000);
      waitForPending(tagsForTwoToSink, 0, 2000);
      waitForPending(tagsForAllToSink, 0, 100);

    } finally {
      controller.stop();
    }
  }

  private static long waitForPending(Map<String, String> tags, long expected, long millis)
    throws Exception {
    return waitForPending(tags, expected, null, millis);
  }

  private static long waitForPending(Map<String, String> tags, long expected, Long alternative, long millis)
    throws Exception {
    long pending = 0L;
    while (millis >= 0) {
      pending = getPending(tags);
      if (pending == expected || alternative != null && pending == alternative) {
        return pending;
      }
      TimeUnit.MILLISECONDS.sleep(50);
      millis -= 50;
    }
    throw new RuntimeException("Timeout reached waiting for pending to reach " + expected
                                 + (alternative == null ? "" : " or " + alternative)
                                 + " for " + tags + "(actual value is " + pending + ")");
  }

  private static long getPending(Map<String, String> tags) throws Exception {
    MetricDataQuery metricDataQuery =
      new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, "system.queue.pending",
                          AggregationFunction.SUM, tags, ImmutableList.<String>of());
    Collection<MetricTimeSeries> query = metricStore.query(metricDataQuery);
    if (query.isEmpty()) {
      return 0;
    }
    MetricTimeSeries timeSeries = Iterables.getOnlyElement(query);
    List<TimeValue> timeValues = timeSeries.getTimeValues();
    TimeValue timeValue = Iterables.getOnlyElement(timeValues);
    return timeValue.getValue();
  }

  private static Map<String, String> metricTagsForQueue(String producer, String queue, String consumer) {
    Map<String, String> tags = Maps.newHashMap();
    tags.put(Constants.Metrics.Tag.NAMESPACE, DefaultId.NAMESPACE.getId());
    tags.put(Constants.Metrics.Tag.APP, "PendingMetricTestApp");
    tags.put(Constants.Metrics.Tag.FLOW, "TestPendingFlow");
    if (producer != null) {
      tags.put(Constants.Metrics.Tag.PRODUCER, producer);
    }
    if (queue != null) {
      tags.put(Constants.Metrics.Tag.FLOWLET_QUEUE, queue);
    }
    if (consumer != null) {
      tags.put(Constants.Metrics.Tag.CONSUMER, consumer);
    }
    return tags;
  }
}
