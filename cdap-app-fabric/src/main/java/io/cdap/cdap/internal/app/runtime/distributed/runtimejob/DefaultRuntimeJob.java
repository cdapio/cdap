/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.runtimejob;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DefaultProgramRunnerFactory;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJob;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobEnvironment;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of a {@link RuntimeJob}. This class is responsible for submitting cdap program to a
 * {@link TwillRunner} provided by {@link RuntimeJobEnvironment}.
 */
public class DefaultRuntimeJob implements RuntimeJob {
  private static final String PROGRAM_OPTIONS_FILE_NAME = "program.options.json";
  private static final String CDAP_CONF_FILE_NAME = "cConf.xml";
  private static final String APP_SPEC_FILE_NAME = "appSpec.json";
  private static final Gson GSON =
    ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec()).create();

  @Override
  public void run(RuntimeJobEnvironment runtimeJobEnv) throws Exception {
    // Get Program Options
    ProgramOptions programOptions = readJsonFile(new File(PROGRAM_OPTIONS_FILE_NAME), ProgramOptions.class);
    ProgramId programId = programOptions.getProgramId();

    // Get App spec
    ApplicationSpecification appSpec = readJsonFile(new File(APP_SPEC_FILE_NAME), ApplicationSpecification.class);
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);

    // Create cConf with provided properties. These properties can be used to set configs for twill runner such as
    // connection string for discovery.
    CConfiguration cConf = createCConf(runtimeJobEnv);

    // Create injector and get program runner
    Injector injector = Guice.createInjector(createmodule(runtimeJobEnv, cConf));
    ProgramRunner programRunner = injector.getInstance(ProgramRunnerFactory.class).create(programId.getType());

    // Create and run the program. The program files should be present in current working directory.
    try (Program program = createProgram(cConf, programRunner, programDescriptor, programOptions)) {
      CompletableFuture<ProgramController.State> programCompletion = new CompletableFuture<>();
      ProgramController controller = programRunner.run(program, programOptions);
      controller.addListener(new AbstractListener() {
        @Override
        public void completed() {
          programCompletion.complete(ProgramController.State.COMPLETED);
        }

        @Override
        public void killed() {
          programCompletion.complete(ProgramController.State.KILLED);
        }

        @Override
        public void error(Throwable cause) {
          programCompletion.completeExceptionally(cause);
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      // Block on the completion
      programCompletion.get();
    } finally {
      if (programRunner instanceof Closeable) {
        ((Closeable) programRunner).close();
      }
    }
  }

  private CConfiguration createCConf(RuntimeJobEnvironment runtimeJobEnv) throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.clear();
    cConf.addResource(new File(CDAP_CONF_FILE_NAME).toURI().toURL());
    for (Map.Entry<String, String> entry : runtimeJobEnv.getProperties().entrySet()) {
      cConf.set(entry.getKey(), entry.getValue());
    }
    return cConf;
  }

  private static <T> T readJsonFile(File file, Class<T> type) {
    try (Reader reader = new BufferedReader(new FileReader(file))) {
      return GSON.fromJson(reader, type);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to read %s file at %s", type.getSimpleName(), file.getAbsolutePath()), e);
    }
  }

  private Program createProgram(CConfiguration cConf, ProgramRunner programRunner,
                                ProgramDescriptor programDescriptor, ProgramOptions options) throws IOException {
    File tempDir = createTempDirectory(cConf, options.getProgramId(), options.getArguments()
      .getOption(ProgramOptionConstants.RUN_ID));
    File programDir = new File(tempDir, "program");
    DirUtils.mkdirs(programDir);
    File programJarFile = new File(programDir, "program.jar");
    Location programJarLocation = Locations.toLocation(
      new File(options.getArguments().getOption(ProgramOptionConstants.PROGRAM_JAR)));
    Locations.linkOrCopy(programJarLocation, programJarFile);
    programJarLocation = Locations.toLocation(programJarFile);
    BundleJarUtil.unJar(programJarLocation, programDir);

    return Programs.create(cConf, programRunner, programDescriptor, programJarLocation, programDir);
  }

  private File createTempDirectory(CConfiguration cConf, ProgramId programId, String runId) {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR)).getAbsoluteFile();
    File dir = new File(tempDir, String.format("%s.%s.%s.%s.%s",
                                               programId.getType().name().toLowerCase(),
                                               programId.getNamespace(), programId.getApplication(),
                                               programId.getProgram(), runId));
    DirUtils.mkdirs(dir);
    return dir;
  }

  /**
   * Returns list of guice modules used to start the program run.
   */
  private List<Module> createmodule(RuntimeJobEnvironment runtimeJobEnv, CConfiguration cConf) {
    List<Module> modules = new ArrayList<>();
    modules.add(new AuthenticationContextModules().getProgramContainerModule());
    modules.add(new MessagingClientModule());
    modules.add(new DFSLocationModule());
    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);
        bind(CConfiguration.class).toInstance(cConf);
        bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

        // get twill runner from environment
        bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class)
          .toInstance(runtimeJobEnv.getTwillRunner());
        // get discovery service from environment
        bind(DiscoveryService.class).toInstance(runtimeJobEnv.getDiscoveryService());
        // get discovery service client from environment
        bind(DiscoveryServiceClient.class).toInstance(runtimeJobEnv.getDiscoveryServiceClient());

        MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
          binder(), ProgramType.class, ProgramRunner.class);

        bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.DISTRIBUTED);
        bind(ProgramRunnerFactory.class).annotatedWith(Constants.AppFabric.ProgramRunner.class)
          .to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
        bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class).in(Scopes.SINGLETON);

        defaultProgramRunnerBinder.addBinding(ProgramType.MAPREDUCE).to(DistributedMapReduceProgramRunner.class);
        defaultProgramRunnerBinder.addBinding(ProgramType.WORKFLOW).to(DistributedWorkflowProgramRunner.class);
        defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);
        bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
      }
    });

    return modules;
  }
}
