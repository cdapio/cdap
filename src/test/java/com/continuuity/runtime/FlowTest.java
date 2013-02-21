package com.continuuity.runtime;

import com.continuuity.TestHelper;
import com.continuuity.WordCountApp;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.Id;
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
import com.continuuity.common.logging.common.LocalLogWriter;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntryImpl;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.FlowProgramRunner;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;

import java.nio.ByteBuffer;
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

    LocalLocationFactory lf = new LocalLocationFactory();

    Location deployedJar = lf.create(
      JarFinder.getJar(WordCountApp.class, TestHelper.getManifestWithMainClass(WordCountApp.class))
    );
    deployedJar.deleteOnExit();

    ListenableFuture<?> p = TestHelper.getLocalManager(configuration).deploy(Id.Account.DEFAULT(), deployedJar);
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
    OperationContext opCtx = new OperationContext(Id.Account.DEFAULT().getId(),
                                                  app.getAppSpecLoc().getSpecification().getName());

    QueueProducer queueProducer = new QueueProducer("Testing");
    QueueName queueName = QueueName.fromStream(Id.Account.DEFAULT(), "text");
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

    controller.stop().get();
  }
}
