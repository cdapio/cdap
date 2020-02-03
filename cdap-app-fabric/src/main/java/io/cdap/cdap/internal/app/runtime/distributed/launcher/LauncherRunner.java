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

package io.cdap.cdap.internal.app.runtime.distributed.launcher;


import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.util.Modules;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DefaultProgramRunnerFactory;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRuntimeService;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class LauncherRunner {
  private static final Logger LOG = LoggerFactory.getLogger(LauncherRunner.class);

//  public void runnerMethod() throws Exception {
//    YarnConfiguration conf = new YarnConfiguration();
//    LocationFactory locationFactory = new FileContextLocationFactory(conf);
//    TwillRunnerService twillRunner = new YarnTwillRunnerService(conf, "localhost:2181", locationFactory);
//    twillRunner.start();
//
//    try {
//      TwillController controller = twillRunner.prepare(new WorkerTwillRunnable("TestWorker"))
//        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
//        .start();
//
//      LOG.info("Job submitted");
//      controller.awaitTerminated();
//      LOG.info("Job completed");
//    } finally {
//      twillRunner.stop();
//    }
//  }

//  public void runnerMethod() throws Exception {
//    Injector injector = Guice.createInjector(createmodule());
//    CConfiguration cConf = injector.getInstance(CConfiguration.class);
//    ProgramRunnerFactory runnerFactory = injector.getInstance(ProgramRunnerFactory.class);
//    DistributedWorkerProgramRunner programRunner =
//      (DistributedWorkerProgramRunner) runnerFactory.create(ProgramType.WORKER);
//
//
//    ProgramId programId = new ProgramId("default", "TestWorkerApp", ProgramType.WORKER, "TestWorker");
//    RunId runId = RunIds.generate();
//
//    File tempDir = createTempDirectory(cConf, programId, runId);
//    // Get the artifact details and save it into the program options.
//    ArtifactId artifactId = new ArtifactId("default", "workerapp", "1.0.0-SNAPSHOT");
//    ProgramOptions runtimeProgramOptions = updateProgramOptions(artifactId, programId, options, runId);
//
//    // Create and run the program
//    Program executableProgram = createProgram(cConf, programRunner, programDescriptor, artifactDetail, tempDir);
//    ProgramController controller = programRunner.run(executableProgram, runtimeProgramOptions);
//  }

  /**
   * Creates a {@link Program} for the given {@link ProgramRunner} from the given program jar {@link Location}.
   */
  protected Program createProgram(CConfiguration cConf, ProgramRunner programRunner,
                                  ProgramDescriptor programDescriptor,
                                  ArtifactDetail artifactDetail, final File tempDir) throws IOException {

    final Location programJarLocation = artifactDetail.getDescriptor().getLocation();

    // Take a snapshot of the JAR file to avoid program mutation
    final File unpackedDir = new File(tempDir, "unpacked");
    unpackedDir.mkdirs();
    try {
      File programJar = Locations.linkOrCopy(programJarLocation, new File(tempDir, "program.jar"));
      // Unpack the JAR file
      BundleJarUtil.unJar(programJar, unpackedDir);
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      // should not happen
      throw Throwables.propagate(e);
    }
    return Programs.create(cConf, programRunner, programDescriptor, programJarLocation, unpackedDir);
  }

  private ProgramOptions updateProgramOptions(ArtifactId artifactId, ProgramId programId,
                                              ProgramOptions options, RunId runId) {
    // Build the system arguments
    Map<String, String> systemArguments = new HashMap<>(options.getArguments().asMap());
    systemArguments.putIfAbsent(ProgramOptionConstants.RUN_ID, runId.getId());
    systemArguments.putIfAbsent(ProgramOptionConstants.ARTIFACT_ID, Joiner.on(':').join(artifactId.toIdParts()));

    // Resolves the user arguments
    // First resolves at the cluster scope if the cluster.name is not empty
    String clusterName = options.getArguments().getOption(Constants.CLUSTER_NAME);
    Map<String, String> userArguments = options.getUserArguments().asMap();
    if (!Strings.isNullOrEmpty(clusterName)) {
      userArguments = RuntimeArguments.extractScope("cluster", clusterName, userArguments);
    }
    // Then resolves at the application scope
    userArguments = RuntimeArguments.extractScope("app", programId.getApplication(), userArguments);
    // Then resolves at the program level
    userArguments = RuntimeArguments.extractScope(programId.getType().getScope(), programId.getProgram(),
                                                  userArguments);

    return new SimpleProgramOptions(options.getProgramId(), new BasicArguments(systemArguments),
                                    new BasicArguments(userArguments), options.isDebug());
  }

  /**
   * Creates a local temporary directory for this program run.
   */
  private File createTempDirectory(CConfiguration cConf, ProgramId programId, RunId runId) {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File dir = new File(tempDir, String.format("%s.%s.%s.%s.%s",
                                               programId.getType().name().toLowerCase(),
                                               programId.getNamespace(), programId.getApplication(),
                                               programId.getProgram(), runId.getId()));
    dir.mkdirs();
    return dir;
  }

  private Module createmodule() {
    Module module = new PrivateModule() {

      @Override
      protected void configure() {
        // Bind ProgramRunnerFactory and expose it
        // ProgramRunnerFactory should be in distributed mode
        bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.DISTRIBUTED);
        // Bind and expose ProgramRunnerFactory. It is used in both program deployment and program execution.
        // Should get refactory by CDAP-5506
        bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
        expose(ProgramRunnerFactory.class);


        // The following are bindings are for ProgramRunners. They are private to this module and only
        // available to the remote execution ProgramRunnerFactory exposed.

        // This set of program runners are for on_premise mode
        bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);
        // TwillRunner used by the ProgramRunner is the remote execution one
        bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class).to(TwillRunner.class);
        // ProgramRunnerFactory used by ProgramRunner is the remote execution one.
        bind(ProgramRunnerFactory.class)
          .annotatedWith(Constants.AppFabric.ProgramRunner.class)
          .to(ProgramRunnerFactory.class);

        // Bind ProgramRunner
        MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder =
          MapBinder.newMapBinder(binder(), ProgramType.class, ProgramRunner.class);
        defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);

        // Bind and expose ProgramRuntimeService
        bind(ProgramRuntimeService.class).to(DistributedProgramRuntimeService.class).in(Scopes.SINGLETON);
        expose(ProgramRuntimeService.class);
      }
    };

    return Modules.combine(module,
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class)
                                 .in(Scopes.SINGLETON);
                             }
                           });
  }
}
