package com.continuuity.runtime;

import com.continuuity.CountAndFilterWord;
import com.continuuity.TestCountRandomApp;
import com.continuuity.TestHelper;
import com.continuuity.WordCountApp;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.DefaultId;
import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntryImpl;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.discovery.Discoverable;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.discovery.DiscoveryServiceClient;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.flow.FlowProgramRunner;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FlowTest {

  @Test
  public void testFlow() throws Exception {
    final CConfiguration configuration = CConfiguration.create();
    configuration.set("app.temp.dir", "/tmp/app/temp");
    configuration.set("app.output.dir", "/tmp/app/archive" + UUID.randomUUID());

    Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                             new BigMamaModule(configuration));

    injector.getInstance(DiscoveryService.class).startAndWait();

    LocalLocationFactory lf = new LocalLocationFactory();

    Location deployedJar = lf.create(
      JarFinder.getJar(WordCountApp.class, TestHelper.getManifestWithMainClass(WordCountApp.class))
    );
    deployedJar.deleteOnExit();

    ListenableFuture<?> p = TestHelper.getLocalManager(configuration).deploy(DefaultId.ACCOUNT, deployedJar);
    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
    final ApplicationWithPrograms app = (ApplicationWithPrograms)p.get();
    List<ProgramController> controllers = Lists.newArrayList();
    for (final Program program : app.getPrograms()) {
      ProgramRunner runner = runnerFactory.create(ProgramRunnerFactory.Type.valueOf(program.getProcessorType().name()));
      controllers.add(runner.run(program, new ProgramOptions() {
        @Override
        public String getName() {
          return program.getProgramName();
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
    OperationExecutor opex = injector.getInstance(OperationExecutor.class);
    OperationContext opCtx = new OperationContext(DefaultId.ACCOUNT.getId(),
                                                  app.getAppSpecLoc().getSpecification().getName());

    QueueProducer queueProducer = new QueueProducer("Testing");
    QueueName queueName = QueueName.fromStream(DefaultId.ACCOUNT, "text");
    StreamEventCodec codec = new StreamEventCodec();
    for (int i = 0; i < 10; i++) {
      String msg = "Testing message " + i;
      StreamEvent event = new DefaultStreamEvent(ImmutableMap.<String, String>of(),
                                                 ByteBuffer.wrap(msg.getBytes(Charsets.UTF_8)));
      QueueEnqueue enqueue = new QueueEnqueue(queueProducer, queueName.toBytes(),
                                              new QueueEntryImpl(codec.encodePayload(event)));
      opex.commit(opCtx, enqueue);
    }

    TimeUnit.SECONDS.sleep(5);

    // Query
    Gson gson = new Gson();
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    discoveryServiceClient.startAndWait();
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

    Assert.assertEquals(ImmutableMap.of("text:Testing", 10L), responseContent);

    client.getConnectionManager().shutdown();

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }

  @Test
  public void testCountRandomApp() throws Exception {
    final CConfiguration configuration = CConfiguration.create();
    configuration.set("app.temp.dir", "/tmp/app/temp");
    configuration.set("app.output.dir", "/tmp/app/archive" + UUID.randomUUID());

    Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                             new BigMamaModule(configuration));

    LocalLocationFactory lf = new LocalLocationFactory();

    Location deployedJar = lf.create(
      JarFinder.getJar(TestCountRandomApp.class, TestHelper.getManifestWithMainClass(TestCountRandomApp.class))
    );
    deployedJar.deleteOnExit();

    ListenableFuture<?> p = TestHelper.getLocalManager(configuration).deploy(DefaultId.ACCOUNT, deployedJar);
    final ApplicationWithPrograms app = (ApplicationWithPrograms)p.get();
    ProgramController controller = null;
    for (final Program program : app.getPrograms()) {
      if (program.getProcessorType() == Type.FLOW) {
        ProgramRunner runner = injector.getInstance(FlowProgramRunner.class);
        controller = runner.run(program, new ProgramOptions() {
          @Override
          public String getName() {
            return program.getProgramName();
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
    final CConfiguration configuration = CConfiguration.create();
    configuration.set("app.temp.dir", "/tmp/app/temp");
    configuration.set("app.output.dir", "/tmp/app/archive" + UUID.randomUUID());

    Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                             new BigMamaModule(configuration));

    LocalLocationFactory lf = new LocalLocationFactory();

    Location deployedJar = lf.create(
      JarFinder.getJar(CountAndFilterWord.class, TestHelper.getManifestWithMainClass(CountAndFilterWord.class))
    );
    deployedJar.deleteOnExit();

    ListenableFuture<?> p = TestHelper.getLocalManager(configuration).deploy(DefaultId.ACCOUNT, deployedJar);
    final ApplicationWithPrograms app = (ApplicationWithPrograms)p.get();
    ProgramController controller = null;
    for (final Program program : app.getPrograms()) {
      if (program.getProcessorType() == Type.FLOW) {
        ProgramRunner runner = injector.getInstance(FlowProgramRunner.class);
        controller = runner.run(program, new ProgramOptions() {
          @Override
          public String getName() {
            return program.getProgramName();
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
    OperationExecutor opex = injector.getInstance(OperationExecutor.class);
    OperationContext opCtx = new OperationContext(DefaultId.ACCOUNT.getId(),
                                                  app.getAppSpecLoc().getSpecification().getName());

    QueueProducer queueProducer = new QueueProducer("Testing");
    QueueName queueName = QueueName.fromStream(DefaultId.ACCOUNT, "text");
    StreamEventCodec codec = new StreamEventCodec();
    for (int i = 0; i < 1; i++) {
      String msg = "Testing message " + i;
      StreamEvent event = new DefaultStreamEvent(ImmutableMap.<String, String>of("title", "test"),
                                                 ByteBuffer.wrap(msg.getBytes(Charsets.UTF_8)));
      QueueEnqueue enqueue = new QueueEnqueue(queueProducer, queueName.toBytes(),
                                              new QueueEntryImpl(codec.encodePayload(event)));
      opex.commit(opCtx, enqueue);
    }

    TimeUnit.SECONDS.sleep(5);

    controller.stop().get();
  }
}
