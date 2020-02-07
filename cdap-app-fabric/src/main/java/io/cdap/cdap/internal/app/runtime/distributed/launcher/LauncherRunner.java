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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.yarn.YarnTwillRunnerService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;

/**
 *
 */
public class LauncherRunner {
  //private static final Logger LOG = LoggerFactory.getLogger(LauncherRunner.class);
  private static final Gson GSON =
    ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec()).create();

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

  public void runnerMethod(String[] args) throws Exception {
    Injector injector = Guice.createInjector(createmodule());
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    DistributedWorkerProgramRunner programRunner = injector.getInstance(DistributedWorkerProgramRunner.class);

    String runIdString = args[0];
    ProgramId programId = new ProgramId("default", "TestWorkerAppTwo", ProgramType.WORKER, "TestWorker");


    File tempDir = createTempDirectory(cConf, programId, runIdString);
    // Get the artifact details and save it into the program options.
    ArtifactId artifactId = new ArtifactId("default", "workerapp", "2.0.0-SNAPSHOT");

    String pwd = System.getProperty("user.dir");
    System.out.println("Current working directory " + pwd);

    System.out.println("Reading program options json");
    File file = new File("program.options.json");
    if (!file.exists()) {
      System.out.println("File program options does not exist " + file.getAbsoluteFile());
    }
    //init array with file length
    byte[] bytesArray = new byte[(int) file.length()];

    FileInputStream fis = new FileInputStream(file);
    fis.read(bytesArray); //read file into bytes[]
    fis.close();
    String programOptions = Bytes.toString(bytesArray);

    //System.out.println("Deserialized program options: " + programOptions);

    ProgramOptions deserializedOptions = GSON.fromJson(programOptions, SimpleProgramOptions.class);
    ProgramOptions runtimeProgramOptions = updateProgramOptions(artifactId, programId,
                                                                deserializedOptions, runIdString);

    System.out.println("Reading app spec json");
    file = new File("appSpec.json");

    if (!file.exists()) {
      System.out.println("File program options does not exist " + file.getAbsoluteFile());
      throw new Exception("program options file not found");
    }
    //init array with file length
    bytesArray = new byte[(int) file.length()];

    fis = new FileInputStream(file);
    fis.read(bytesArray); //read file into bytes[]
    fis.close();
    String appSpecJson = Bytes.toString(bytesArray);
    //System.out.println("Deserialized app spec json: " + appSpecJson);

    ApplicationSpecification spec = GSON.fromJson(appSpecJson, ApplicationSpecification.class);
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, spec);

    // Create and run the program
    System.out.println("Argument 1: " + args[1]);
    Program executableProgram = createProgram(cConf, programRunner, programDescriptor, tempDir, args[1]);
    ProgramController controller = programRunner.run(executableProgram, runtimeProgramOptions);
    CountDownLatch latch = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {

      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        System.out.println("In controller init");
      }

      @Override
      public void completed() {
        System.out.println("Program completed");
        latch.countDown();
      }

      @Override
      public void stopping() {
        System.out.println("Program stopping");
      }

      @Override
      public void killed() {
        System.out.println("Program was killed");
        latch.countDown();
      }

      @Override
      public void error(Throwable cause) {
        latch.countDown();
        System.out.println("Program in error state: " + cause.getMessage());
        cause.printStackTrace();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    latch.await();

    if (controller.getFailureCause() != null) {
      System.out.println("Error message: " + controller.getFailureCause().getMessage());
      controller.getFailureCause().printStackTrace();
      throw new RuntimeException(controller.getFailureCause());
    }
  }

  /**
   * Creates a {@link Program} for the given {@link ProgramRunner} from the given program jar {@link Location}.
   */
  protected Program createProgram(CConfiguration cConf, ProgramRunner programRunner,
                                  ProgramDescriptor programDescriptor,
                                  File tempDir, String runId) throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(new File(System.getProperty("user.dir")));
    Location programJarFileLocation = locationFactory.create("2.0.0-SNAPSHOT." + runId + ".jar");

   // File programJarFileLocation = new File("2.0.0-SNAPSHOT." + runId + ".jar");
    // Take a snapshot of the JAR file to avoid program mutation
    final File unpackedDir = new File(tempDir, "unpacked");
    unpackedDir.mkdirs();
    try {
      File targetPath = new File(tempDir, "program.jar");
      File programJar = Locations.linkOrCopy(programJarFileLocation, targetPath);
      // Unpack the JAR file
      BundleJarUtil.unJar(programJar, unpackedDir);
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      // should not happen
      throw Throwables.propagate(e);
    }

    return Programs.create(cConf, programRunner, programDescriptor, programJarFileLocation, unpackedDir);
  }

  private ProgramOptions updateProgramOptions(ArtifactId artifactId, ProgramId programId,
                                              ProgramOptions options, String runId) {
    // Build the system arguments
    Map<String, String> systemArguments = new HashMap<>(options.getArguments().asMap());
    systemArguments.putIfAbsent(ProgramOptionConstants.RUN_ID, runId);
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
  private File createTempDirectory(CConfiguration cConf, ProgramId programId, String runId) {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    File dir = new File(tempDir, String.format("%s.%s.%s.%s.%s",
                                               programId.getType().name().toLowerCase(),
                                               programId.getNamespace(), programId.getApplication(),
                                               programId.getProgram(), runId));
    dir.mkdirs();
    return dir;
  }

  private List<Module> createmodule() {
    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(CConfiguration.create()));
//    modules.add(new MessagingClientModule());
    modules.add(new AuthenticationContextModules().getProgramContainerModule());
    modules.add(new AbstractModule() {

      @Override
      protected void configure() {
        bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);
        // TwillRunner used by the ProgramRunner is the remote execution one
        YarnConfiguration conf = new YarnConfiguration();
        LocationFactory locationFactory = new FileContextLocationFactory(conf);
        YarnTwillRunnerService yarnTwillRunnerService =
          new YarnTwillRunnerService(conf, "localhost:2181", locationFactory);
        yarnTwillRunnerService.start();
        bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class)
          .toInstance(yarnTwillRunnerService);

       // bind(ProgramStateWriter.class).to(NoOpProgramStateWriter.class).in(Scopes.SINGLETON);
        bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

        // A private Map binding of ProgramRunner for ProgramRunnerFactory to use
        MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
          binder(), ProgramType.class, ProgramRunner.class);

        defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);
      }
    });

    return modules;
  }
}
