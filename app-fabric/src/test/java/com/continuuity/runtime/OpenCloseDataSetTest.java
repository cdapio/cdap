package com.continuuity.runtime;

import com.continuuity.DummyAppWithTrackingTable;
import com.continuuity.TrackingTable;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.queue.QueueName;
import com.continuuity.common.stream.DefaultStreamEvent;
import com.continuuity.common.stream.StreamEventCodec;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.data2.queue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.test.XSlowTests;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.continuuity.test.internal.DefaultId;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * tests that flowlets, procedures and batch jobs close their data sets.
 */
@Category(XSlowTests.class)
public class OpenCloseDataSetTest {

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

  @Test(timeout = 120000)
  public void testDataSetsAreClosed() throws Exception {
    TrackingTable.resetTracker();
    ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(DummyAppWithTrackingTable.class,
                                                                                   TEMP_FOLDER_SUPPLIER);
    ProgramRunnerFactory runnerFactory = AppFabricTestHelper.getInjector().getInstance(ProgramRunnerFactory.class);
    List<ProgramController> controllers = Lists.newArrayList();

    // start the flow and procedure
    for (Program program : app.getPrograms()) {
      if (program.getType().equals(Type.MAPREDUCE)) {
        continue;
      }
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      controllers.add(runner.run(program, new SimpleProgramOptions(program)));
    }

    // write some data to queue
    TransactionSystemClient txSystemClient = AppFabricTestHelper.getInjector().
      getInstance(TransactionSystemClient.class);

    QueueName queueName = QueueName.fromStream("xx");
    QueueClientFactory queueClientFactory = AppFabricTestHelper.getInjector().getInstance(QueueClientFactory.class);
    Queue2Producer producer = queueClientFactory.createProducer(queueName);

    // start tx to write in queue in tx
    Transaction tx = txSystemClient.startShort();
    ((TransactionAware) producer).startTx(tx);

    StreamEventCodec codec = new StreamEventCodec();
    for (int i = 0; i < 4; i++) {
      String msg = "x" + i;
      StreamEvent event = new DefaultStreamEvent(ImmutableMap.<String, String>of(),
                                                 ByteBuffer.wrap(msg.getBytes(Charsets.UTF_8)));
      producer.enqueue(new QueueEntry(codec.encodePayload(event)));
    }

    // commit tx
    ((TransactionAware) producer).commitTx();
    txSystemClient.commit(tx);

    while (TrackingTable.getTracker("foo", "write") < 4) {
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // get the number of writes to the foo table
    Assert.assertEquals(4, TrackingTable.getTracker("foo", "write"));
    // only the flow has started with s single flowlet (procedure is loaded lazily on 1sy request
    Assert.assertEquals(1, TrackingTable.getTracker("foo", "open"));

    // now send a request to the procedure
    Gson gson = new Gson();
    DiscoveryServiceClient discoveryServiceClient = AppFabricTestHelper.getInjector().
      getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = discoveryServiceClient.discover(
      String.format("procedure.%s.%s.%s", DefaultId.ACCOUNT.getId(), "dummy", "DummyProcedure")).iterator().next();

    HttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(String.format("http://%s:%d/apps/%s/procedures/%s/methods/%s",
                                               discoverable.getSocketAddress().getHostName(),
                                               discoverable.getSocketAddress().getPort(),
                                               "dummy",
                                               "DummyProcedure",
                                               "get"));
    post.setEntity(new StringEntity(gson.toJson(ImmutableMap.of("key", "x1"))));
    HttpResponse response = client.execute(post);
    String responseContent = gson.fromJson(
      new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8), String.class);
    client.getConnectionManager().shutdown();
    Assert.assertEquals("x1", responseContent);

    // now the dataset must have a read and another open operation
    Assert.assertEquals(1, TrackingTable.getTracker("foo", "read"));
    Assert.assertEquals(2, TrackingTable.getTracker("foo", "open"));
    Assert.assertEquals(0, TrackingTable.getTracker("foo", "close"));

    // stop flow and procedure, they shuld both close the data set foo
    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
    Assert.assertEquals(2, TrackingTable.getTracker("foo", "close"));

    // now start the m/r job
    // start the flow and procedure
    ProgramController controller = null;
    for (Program program : app.getPrograms()) {
      if (program.getType().equals(Type.MAPREDUCE)) {
        ProgramRunner runner = runnerFactory.create(
          ProgramRunnerFactory.Type.valueOf(program.getType().name()));
        controller = runner.run(program, new SimpleProgramOptions(program));
      }
    }
    Assert.assertNotNull(controller);

    while (!controller.getState().equals(ProgramController.State.STOPPED)) {
      TimeUnit.MILLISECONDS.sleep(100);
    }

    // M/r job is done, one mapper and the m/r client should have opened and closed the data set foo
    // we don't know the exact number of times opened, but it is at least once, and it must be closed the same number
    // of times.
    Assert.assertTrue(2 < TrackingTable.getTracker("foo", "open"));
    Assert.assertEquals(TrackingTable.getTracker("foo", "open"), TrackingTable.getTracker("foo", "close"));
    Assert.assertTrue(0 < TrackingTable.getTracker("bar", "open"));
    Assert.assertEquals(TrackingTable.getTracker("bar", "open"), TrackingTable.getTracker("bar", "close"));

  }
}
