package com.continuuity.runtime;

import com.continuuity.ArgumentCheckApp;
import com.continuuity.CountAndFilterWord;
import com.continuuity.InvalidFlowOutputApp;
import com.continuuity.TestCountRandomApp;
import com.continuuity.WordCountApp;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.queue.QueueName;
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
import com.continuuity.internal.app.runtime.flow.FlowProgramRunner;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FlowTest {

  private static final Logger LOG = LoggerFactory.getLogger(FlowTest.class);

  @Test
  public void testAppWithArgs() throws Exception {
   final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(ArgumentCheckApp.class);
   ProgramRunnerFactory runnerFactory = TestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    // Only running flow is good. But, in case procedure, we need to send something to procedure as it's lazy
    // load on procedure.
    List<ProgramController> controllers = Lists.newArrayList();
    for (final Program program : app.getPrograms()) {
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      controllers.add(runner.run(program, new ProgramOptions() {
        @Override
        public String getName() {
          return program.getName();
        }

        @Override
        public Arguments getArguments() {
          return new BasicArguments();
        }

        @Override
        public Arguments getUserArguments() {
          return new BasicArguments(ImmutableMap.<String, String>of("arg", "test"));
        }
      }));
    }

    TimeUnit.SECONDS.sleep(1);

    Gson gson = new Gson();
    DiscoveryServiceClient discoveryServiceClient = TestHelper.getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = discoveryServiceClient.discover(
      String.format("procedure.%s.%s.%s",
                    DefaultId.ACCOUNT.getId(), "ArgumentCheckApp", "SimpleProcedure")).iterator().next();

    HttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(String.format("http://%s:%d/apps/%s/procedures/%s/%s",
                                               discoverable.getSocketAddress().getHostName(),
                                               discoverable.getSocketAddress().getPort(),
                                               "ArgumentCheckApp",
                                               "SimpleProcedure",
                                               "argtest"));
    post.setEntity(new StringEntity(gson.toJson(ImmutableMap.of("word", "text:Testing"))));
    HttpResponse response = client.execute(post);
    Assert.assertTrue(response.getStatusLine().getStatusCode() == 200);

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }

  @Test
  public void testFlow() throws Exception {
    final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(WordCountApp.class);
    ProgramRunnerFactory runnerFactory = TestHelper.getInjector().getInstance(ProgramRunnerFactory.class);

    List<ProgramController> controllers = Lists.newArrayList();

    for (final Program program : app.getPrograms()) {
      // running mapreduce is out of scope of this tests (there's separate unit-test for that)
      if (program.getType() == Type.MAPREDUCE) {
        continue;
      }
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getType().name()));
      controllers.add(runner.run(program, new ProgramOptions() {
        @Override
        public String getName() {
          return program.getName();
        }

        @Override
        public Arguments getArguments() {
          return new BasicArguments();
        }

        @Override
        public Arguments getUserArguments() {
          return new BasicArguments();
        }
      }));
    }

    TimeUnit.SECONDS.sleep(1);

    TransactionSystemClient txSystemClient = TestHelper.getInjector().getInstance(TransactionSystemClient.class);

    QueueName queueName = QueueName.fromStream("text");
    QueueClientFactory queueClientFactory = TestHelper.getInjector().getInstance(QueueClientFactory.class);
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
    DiscoveryServiceClient discoveryServiceClient = TestHelper.getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable discoverable = discoveryServiceClient.discover(
      String.format("procedure.%s.%s.%s",
                    DefaultId.ACCOUNT.getId(), "WordCountApp", "WordFrequency")).iterator().next();

    HttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(String.format("http://%s:%d/apps/%s/procedures/%s/%s",
                                               discoverable.getSocketAddress().getHostName(),
                                               discoverable.getSocketAddress().getPort(),
                                               "WordCountApp",
                                               "WordFrequency",
                                               "wordfreq"));
    post.setEntity(new StringEntity(gson.toJson(ImmutableMap.of("word", "text:Testing"))));
    HttpResponse response = client.execute(post);
    Map<String, Long> responseContent = gson.fromJson(
      new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8),
      new TypeToken<Map<String, Long>>(){}.getType());

    LOG.info("Procedure response: " + responseContent);
    Assert.assertEquals(ImmutableMap.of("text:Testing", 10L), responseContent);

    client.getConnectionManager().shutdown();

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }

  @Test
  public void testCountRandomApp() throws Exception {
    final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(TestCountRandomApp.class);

    System.out.println(ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator())
                                                      .toJson(app.getAppSpecLoc().getSpecification()));

    ProgramController controller = null;
    for (final Program program : app.getPrograms()) {
      if (program.getType() == Type.FLOW) {
        ProgramRunner runner = TestHelper.getInjector().getInstance(FlowProgramRunner.class);
        controller = runner.run(program, new ProgramOptions() {
          @Override
          public String getName() {
            return program.getName();
          }

          @Override
          public Arguments getArguments() {
            return new BasicArguments();
          }

          @Override
          public Arguments getUserArguments() {
            return new BasicArguments();
          }
        });
      }
    }

    TimeUnit.SECONDS.sleep(10);
    controller.stop().get();
  }

  @Test
  public void testCountAndFilterWord() throws Exception {
    final ApplicationWithPrograms app = TestHelper.deployApplicationWithManager(CountAndFilterWord.class);

    ProgramController controller = null;
    for (final Program program : app.getPrograms()) {
      if (program.getType() == Type.FLOW) {
        ProgramRunner runner = TestHelper.getInjector().getInstance(FlowProgramRunner.class);
        controller = runner.run(program, new ProgramOptions() {
          @Override
          public String getName() {
            return program.getName();
          }

          @Override
          public Arguments getArguments() {
            return new BasicArguments();
          }

          @Override
          public Arguments getUserArguments() {
            return new BasicArguments();
          }
        });
      }
    }

    TimeUnit.SECONDS.sleep(1);

    QueueName queueName = QueueName.fromStream("text");
    QueueClientFactory queueClientFactory = TestHelper.getInjector().getInstance(QueueClientFactory.class);
    final Queue2Producer producer = queueClientFactory.createProducer(queueName);

    TransactionExecutorFactory txExecutorFactory =
      TestHelper.getInjector().getInstance(TransactionExecutorFactory.class);
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
      TestHelper.deployApplicationWithManager(InvalidFlowOutputApp.class);
    } catch (Exception e) {
      throw Throwables.getRootCause(e);
    }
  }
}
