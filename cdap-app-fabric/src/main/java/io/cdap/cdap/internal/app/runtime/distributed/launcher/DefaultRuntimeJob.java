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
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.runtimejob.Job;
import io.cdap.cdap.runtime.spi.runtimejob.JobContext;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * this is similar to {@link LauncherRunner}.
 */
public class DefaultRuntimeJob implements Job {
  private static final Gson GSON =
    ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec()).create();

  @Override
  public void run(JobContext context) throws Exception {
    System.setProperty(Constants.AppFabric.SPARK_COMPAT, SparkCompat.SPARK2_2_11.getCompat());

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
    Cluster cluster = GSON.fromJson(deserializedOptions.getArguments().getOption(ProgramOptionConstants.CLUSTER),
                                    Cluster.class);


    // TODO get namespace, appname, program type, program name from program options
    ProgramId programId = deserializedOptions.getProgramId();
    System.out.println("Program type : " + programId.getType());

    Injector injector = Guice.createInjector(createmodule(context, cluster.getName()));
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    for (Map.Entry<String, String> entry : context.getConfigProperties().entrySet()) {
      System.out.println("adding : key = " + entry.getKey() + ", value = " + entry.getValue());
      cConf.set(entry.getKey(), entry.getValue());
    }

    ProgramRunner programRunner;
    if (programId.getType() == ProgramType.WORKER) {
      programRunner = injector.getInstance(DistributedWorkerProgramRunner.class);
    } else if (programId.getType() == ProgramType.WORKFLOW) {
      programRunner = injector.getInstance(DistributedWorkflowProgramRunner.class);
    } else if (programId.getType() == ProgramType.MAPREDUCE) {
      programRunner = injector.getInstance(DistributedMapReduceProgramRunner.class);
    } else {
      // spark
      programRunner = injector.getInstance(ProgramRunnerFactory.class).create(ProgramType.SPARK);
    }

    // TODO: (vinisha) get it from context
    String runIdString = context.getConfigProperties().get("runid");
    Thread.currentThread().getContextClassLoader().getResource("");
    File tempDir = createTempDirectory(cConf, programId, runIdString);
    // Get the artifact details and save it into the program options.
//    ArtifactId artifactId = new ArtifactId("default", "workerapp", "2.0.0-SNAPSHOT");
//
//    ProgramOptions runtimeProgramOptions = updateProgramOptions(artifactId, programId,
//                                                                deserializedOptions, runIdString);

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
    Program executableProgram = createProgram(cConf, programRunner, programDescriptor, tempDir, deserializedOptions);
    ProgramController controller = programRunner.run(executableProgram, deserializedOptions);
    CompletableFuture<ProgramController.State> programCompletion = new CompletableFuture<>();
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        switch (currentState) {
          case ALIVE:
            System.out.println("In alive, calling alive()");
            alive();
            break;
          case COMPLETED:
            System.out.println("In completed, calling completed()");
            completed();
            break;
          case KILLED:
            System.out.println("In killed, calling killed()");
            killed();
            break;
          case ERROR:
            System.out.println("In error, calling error()");
            error(cause);
            break;
        }
      }

      @Override
      public void alive() {
        System.out.println("program is alive");
      }

      @Override
      public void completed() {
        System.out.println("program is completed");
        programCompletion.complete(ProgramController.State.COMPLETED);
      }

      @Override
      public void killed() {
        System.out.println("program is killed");
        programCompletion.complete(ProgramController.State.KILLED);
      }

      @Override
      public void error(Throwable cause) {
        System.out.println("program is error");
        programCompletion.completeExceptionally(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    // Block on the completion
    programCompletion.get();

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
                                  File tempDir, ProgramOptions options) throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(new File(System.getProperty("user.dir")));
    Location programJarFileLocation =
      locationFactory.create(options.getArguments().getOption(ProgramOptionConstants.PROGRAM_JAR));

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

  private List<Module> createmodule(JobContext context, String clusterName) {
    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(CConfiguration.create()));
    modules.add(new AuthenticationContextModules().getProgramContainerModule());
    modules.add(new AbstractModule() {

      @Override
      protected void configure() {
        bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);

        // start zookeeper server
//        System.setProperty("java.net.preferIPv6Addresses", "true");
//        System.setProperty("twill.zk.server.localhost", "true");
//        InMemoryZKServer server = InMemoryZKServer.builder().build();
//        server.startAndWait();
//
//        System.out.println("### hostname: " + clusterName);
//        System.out.println("zookeeper server has started host: " + server.getConnectionStr());
//        // start zookeeper service
//        ZKClientService service = ZKClientServices.delegate(
//          ZKClients.reWatchOnExpire(
//            ZKClients.retryOnFailure(
//              ZKClientService.Builder.of(server.getConnectionStr())
//                .setSessionTimeout(40000)
//                .build(),
//              RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
//            )
//          )
//        );
//        System.out.println("starting zookeeper discovery service");
//        service.start();
//        System.out.println("zookeeper discovery service has started");
//
//        ZKDiscoveryService zkDiscoveryService = new ZKDiscoveryService(service);
//        bind(DiscoveryService.class).toInstance(zkDiscoveryService);
//        bind(DiscoveryServiceClient.class).toInstance(zkDiscoveryService);
//
//        YarnConfiguration conf = new YarnConfiguration();
//        LocationFactory locationFactory = new FileContextLocationFactory(conf);
//
//        YarnTwillRunnerService yarnTwillRunnerService =
//          new YarnTwillRunnerService(conf, server.getConnectionStr(), locationFactory);
//        yarnTwillRunnerService.start();
//        bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class)
//          .toInstance(yarnTwillRunnerService);
//        System.out.println("yarn service has started");

        bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class)
          .toInstance(context.getTwillRunnerSupplier());
        bind(DiscoveryService.class).toInstance(context.getDiscoveryServiceSupplier());
        bind(DiscoveryServiceClient.class).toInstance(context.getDiscoveryServiceClientSupplier());

        bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

        MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
          binder(), ProgramType.class, ProgramRunner.class);

        // ProgramRunnerFactory should be in distributed mode
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

    modules.add(new MessagingClientModule());
    modules.add(new DFSLocationModule());

    return modules;
  }
}
