package com.continuuity.runtime;

import com.continuuity.ArgumentCheckApp;
import com.continuuity.CountAndFilterWord;
import com.continuuity.InvalidFlowOutputApp;
import com.continuuity.TestCountRandomApp;
import com.continuuity.WordCountApp;
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
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionExecutorFactory;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.internal.app.runtime.flow.FlowProgramRunner;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
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

  @Test
  public void testAppWithArgs() throws Exception {
   final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(ArgumentCheckApp.class,
                                                                                        TEMP_FOLDER_SUPPLIER);
   ProgramRunnerFactory runnerFactory = AppFabricTestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    // Only running flow is good. But, in case procedure, we need to send something to procedure as it's lazy
    // load on procedure.
    List<ProgramController> controllers = Lists.newArrayList();
    for (final Program program : app.getPrograms()) {
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      controllers.add(runner.run(program, new SimpleProgramOptions(
        program.getName(), new BasicArguments(), new BasicArguments(ImmutableMap.of("arg", "test")))));
    }

    TimeUnit.SECONDS.sleep(1);

    Gson gson = new Gson();
    DiscoveryServiceClient discoveryServiceClient = AppFabricTestHelper.getInjector().
                                                    getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = discoveryServiceClient.discover(
      String.format("procedure.%s.%s.%s",
                    DefaultId.ACCOUNT.getId(), "ArgumentCheckApp", "SimpleProcedure")).iterator().next();

    URL url = new URL(String.format("http://%s:%d/apps/%s/procedures/%s/methods/%s",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    "ArgumentCheckApp",
                                    "SimpleProcedure",
                                    "argtest"));
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.getOutputStream().write(gson.toJson(ImmutableMap.of("word", "text:Testing")).getBytes(Charsets.UTF_8));
    Assert.assertTrue(urlConn.getResponseCode() == 200);

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
      if (program.getType() == Type.MAPREDUCE) {
        continue;
      }
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      controllers.add(runner.run(program, new SimpleProgramOptions(program)));
    }

    TimeUnit.SECONDS.sleep(1);

    TransactionSystemClient txSystemClient = AppFabricTestHelper.getInjector().
                                             getInstance(TransactionSystemClient.class);

    QueueName queueName = QueueName.fromStream("text");
    QueueClientFactory queueClientFactory = AppFabricTestHelper.getInjector().getInstance(QueueClientFactory.class);
    Queue2Producer producer = queueClientFactory.createProducer(queueName);

    // start tx to write in queue in tx
    Transaction tx = txSystemClient.startShort();
    ((TransactionAware) producer).startTx(tx);

    StreamEventCodec codec = new StreamEventCodec();
    for (int i = 0; i < 10; i++) {
      String msg = "Testing message " + i;
      StreamEvent event = new DefaultStreamEvent(ImmutableMap.<String, String>of(),
                                                 ByteBuffer.wrap(msg.getBytes(Charsets.UTF_8)));
      producer.enqueue(new QueueEntry(codec.encodePayload(event)));
    }

    // commit tx
    ((TransactionAware) producer).commitTx();
    txSystemClient.commit(tx);

    TimeUnit.SECONDS.sleep(10);

    // Procedure
    Gson gson = new Gson();
    DiscoveryServiceClient discoveryServiceClient = AppFabricTestHelper.getInjector().
      getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = discoveryServiceClient.discover(
      String.format("procedure.%s.%s.%s",
                    DefaultId.ACCOUNT.getId(), "WordCountApp", "WordFrequency")).iterator().next();

    URL url = new URL(String.format("http://%s:%d/apps/%s/procedures/%s/methods/%s",
                                    discoverable.getSocketAddress().getHostName(),
                                    discoverable.getSocketAddress().getPort(),
                                    "WordCountApp",
                                    "WordFrequency",
                                    "wordfreq"));
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    urlConn.getOutputStream().write(gson.toJson(ImmutableMap.of("word", "text:Testing")).getBytes(Charsets.UTF_8));
    Map<String, Long> responseContent = gson.fromJson(new InputStreamReader(urlConn.getInputStream(), Charsets.UTF_8),
                                                      new TypeToken<Map<String, Long>>() { }.getType());

    LOG.info("Procedure response: " + responseContent);
    Assert.assertEquals(ImmutableMap.of("text:Testing", 10L), responseContent);

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }

  @Test
  public void testCountRandomApp() throws Exception {
    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(TestCountRandomApp.class,
                                                                                         TEMP_FOLDER_SUPPLIER);
    System.out.println(ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator())
                                                      .toJson(app.getAppSpecLoc().getSpecification()));

    ProgramController controller = null;
    for (final Program program : app.getPrograms()) {
      if (program.getType() == Type.FLOW) {
        ProgramRunner runner = AppFabricTestHelper.getInjector().getInstance(FlowProgramRunner.class);
        controller = runner.run(program, new SimpleProgramOptions(program));
      }
    }

    TimeUnit.SECONDS.sleep(10);
    controller.stop().get();
  }

  @Test
  public void testCountAndFilterWord() throws Exception {
    final ApplicationWithPrograms app = AppFabricTestHelper.deployApplicationWithManager(CountAndFilterWord.class,
                                                                                         TEMP_FOLDER_SUPPLIER);
    ProgramController controller = null;
    for (final Program program : app.getPrograms()) {
      if (program.getType() == Type.FLOW) {
        ProgramRunner runner = AppFabricTestHelper.getInjector().getInstance(FlowProgramRunner.class);
        controller = runner.run(program, new SimpleProgramOptions(program));
      }
    }

    TimeUnit.SECONDS.sleep(1);

    QueueName queueName = QueueName.fromStream("text");
    QueueClientFactory queueClientFactory = AppFabricTestHelper.getInjector().getInstance(QueueClientFactory.class);
    final Queue2Producer producer = queueClientFactory.createProducer(queueName);

    TransactionExecutorFactory txExecutorFactory =
      AppFabricTestHelper.getInjector().getInstance(TransactionExecutorFactory.class);
    TransactionExecutor txExecutor =
      txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) producer));

    StreamEventCodec codec = new StreamEventCodec();
    for (int i = 0; i < 1; i++) {
      String msg = "Testing message " + i;
      StreamEvent event = new DefaultStreamEvent(ImmutableMap.<String, String>of("title", "test"),
                                                 ByteBuffer.wrap(msg.getBytes(Charsets.UTF_8)));
      final QueueEntry entry = new QueueEntry(codec.encodePayload(event));

      txExecutor.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          producer.enqueue(entry);
        }
      });
    }

    TimeUnit.SECONDS.sleep(5);

    controller.stop().get();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testInvalidOutputEmitter() throws Throwable {
    try {
      AppFabricTestHelper.deployApplicationWithManager(InvalidFlowOutputApp.class, TEMP_FOLDER_SUPPLIER);
    } catch (Exception e) {
      throw Throwables.getRootCause(e);
    }
  }
}
