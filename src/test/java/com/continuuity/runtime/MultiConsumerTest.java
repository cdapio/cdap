package com.continuuity.runtime;

import com.continuuity.TestHelper;
import com.continuuity.WordCountApp;
import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.AbstractGeneratorFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.app.DefaultId;
import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MultiConsumerTest {

  public static final class MultiApp implements Application {

    @Override
    public ApplicationSpecification configure() {
      return ApplicationSpecification.Builder.with()
        .setName("MultiApp")
        .setDescription("MultiApp")
        .noStream()
        .noDataSet()
        .withFlows().add(new MultiFlow())
        .noProcedure()
        .build();
    }
  }

  public static final class MultiFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("MultiFlow")
        .setDescription("MultiFlow")
        .withFlowlets()
          .add("gen", new Generator())
          .add("c1", new Consumer())
          .add("c2", new Consumer())
        .connect()
          .from("gen").to("c1")
          .from("gen").to("c2")
        .build();
    }
  }

  public static final class Generator extends AbstractGeneratorFlowlet {

    private OutputEmitter<String> output;

    @Override
    public void generate() throws Exception {
      output.emit("Testing");
    }
  }

  public static final class Consumer extends AbstractFlowlet {
    public void process(String str) {
      System.out.println(getContext().getName() + " " + str);
    }
  }

  @Test
  public void testMulti() throws Exception {
    final CConfiguration configuration = CConfiguration.create();
    configuration.set(Constants.CFG_APP_FABRIC_TEMP_DIR, "/tmp/app/temp");
    configuration.set(Constants.CFG_APP_FABRIC_OUTPUT_DIR, "/tmp/app/archive" + UUID.randomUUID());

    Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                             new BigMamaModule(configuration));

    injector.getInstance(DiscoveryService.class).startAndWait();

    LocalLocationFactory lf = new LocalLocationFactory();

    Location deployedJar = lf.create(
      JarFinder.getJar(WordCountApp.class, TestHelper.getManifestWithMainClass(MultiApp.class))
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

    TimeUnit.MILLISECONDS.sleep(100);

    for (ProgramController controller : controllers) {
      controller.stop().get();
    }
  }
}
