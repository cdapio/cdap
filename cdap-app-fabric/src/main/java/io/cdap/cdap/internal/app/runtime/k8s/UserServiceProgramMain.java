/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.k8s;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.app.guice.DistributedArtifactManagerModule;
import io.cdap.cdap.app.guice.UnsupportedExploreClient;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.namespace.guice.NamespaceQueryAdminModule;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.explore.client.ExploreClient;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.program.StateChangeListener;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactFinder;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.service.ServiceProgramRunner;
import io.cdap.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.master.environment.k8s.AbstractServiceMain;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreClientModule;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.NoOpOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.filesystem.Location;
import org.datanucleus.store.types.backed.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Run a {@link Service}.
 */
public class UserServiceProgramMain extends AbstractServiceMain<ServiceOptions> {
  private static final Logger LOG = LoggerFactory.getLogger(UserServiceProgramMain.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .create();
  private ProgramRunId programRunId;
  private ProgramOptions programOptions;

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(UserServiceProgramMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv, ServiceOptions options) {
    return Arrays.asList(
      new NamespaceQueryAdminModule(),
      new MessagingClientModule(),
      new AuditModule(),
      new AuthorizationEnforcementModule().getDistributedModules(),
      new SecureStoreClientModule(),
      new MetadataReaderWriterModules().getDistributedModules(),
      new DistributedArtifactManagerModule(),
      new AuthenticationContextModules().getProgramContainerModule(),
      // Always use local table implementations, which use LevelDB.
      // In K8s, there won't be HBase and the cdap-site should be set to use SQL store for StructuredTable.
      new DataSetServiceModules().getStandaloneModules(),
      // The Dataset set modules are only needed to satisfy dependency injection
      new DataSetsModules().getDistributedModules(),
      getDataFabricModule(),
      new DFSLocationModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class);
          bind(WorkflowStateWriter.class).to(MessagingWorkflowStateWriter.class);

          // don't need to perform any impersonation from within user programs
          bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

          bind(ServiceAnnouncer.class).toInstance(new ServiceAnnouncer() {
            private final DiscoveryService discoveryService = masterEnv.getDiscoveryServiceSupplier().get();

            @Override
            public Cancellable announce(String serviceName, int port) {
              return discoveryService.register(
                ResolvingDiscoverable.of(new Discoverable(serviceName,
                                                          new InetSocketAddress(options.getBindAddress(), port))));
            }

            @Override
            public Cancellable announce(String serviceName, int port, byte[] payload) {
              return discoveryService.register(
                ResolvingDiscoverable.of(new Discoverable(serviceName,
                                                          new InetSocketAddress(options.getBindAddress(), port),
                                                          payload)));
            }
          });

          bind(ExploreClient.class).to(UnsupportedExploreClient.class);
          bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
        }
      }
    );
  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(ServiceOptions options) {
    File programOptionsFile = new File(options.getProgramOptionsPath());
    programOptions = readJsonFile(programOptionsFile, ProgramOptions.class);
    programRunId = programOptions.getProgramId().run(ProgramRunners.getRunId(programOptions));
    return LoggingContextHelper.getLoggingContextWithRunId(programRunId, programOptions.getArguments().asMap());
  }

  @Override
  protected void addServices(Injector injector, List<? super com.google.common.util.concurrent.Service> services,
                             List<? super AutoCloseable> closeableResources, MasterEnvironment masterEnv,
                             MasterEnvironmentContext masterEnvContext, ServiceOptions options) {
    services.add(injector.getInstance(MetricsCollectionService.class));

    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    ArtifactFinder artifactFinder = injector.getInstance(ArtifactFinder.class);
    ProgramRunner programRunner = injector.getInstance(ServiceProgramRunner.class);

    File appSpecFile = new File(options.getAppSpecPath());
    ApplicationSpecification appSpec = readJsonFile(appSpecFile, ApplicationSpecification.class);

    ProgramStateWriter programStateWriter = injector.getInstance(ProgramStateWriter.class);

    File tableSpecFile = new File(options.getSystemTableSpecsPath());
    List<StructuredTableSpecification> tableSpecifications =
      readJsonFile(tableSpecFile, new TypeToken<List<StructuredTableSpecification>>() { }.getType());
    StructuredTableAdmin tableAdmin = injector.getInstance(StructuredTableAdmin.class);
    StructuredTableRegistry tableRegistry = injector.getInstance(StructuredTableRegistry.class);

    UserService userService = new UserService(cConf, artifactFinder, programRunner, programStateWriter, appSpec,
                                              programOptions, tableSpecifications, programRunId, options,
                                              tableAdmin, tableRegistry);
    services.add(userService);
    closeableResources.add(userService);
  }

  private static ArtifactId convert(NamespaceId namespaceId, io.cdap.cdap.api.artifact.ArtifactId artifactId) {
    NamespaceId namespace = namespaceId;
    if (artifactId.getScope() == ArtifactScope.SYSTEM) {
      namespace = NamespaceId.SYSTEM;
    }
    return namespace.artifact(artifactId.getName(), artifactId.getVersion().getVersion());
  }

  private static <T> T readJsonFile(File file, Class<T> type) {
    try (Reader reader = new BufferedReader(new FileReader(file))) {
      return GSON.fromJson(reader, type);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to read %s file at %s", type.getSimpleName(), file.getAbsolutePath()), e);
    }
  }

  private static <T> List<T> readJsonFile(File file, Type type) {
    try (Reader reader = new BufferedReader(new FileReader(file))) {
      return GSON.fromJson(reader, type);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to read %s file at %s", type.getTypeName(), file.getAbsolutePath()), e);
    }
  }

  /**
   * Service that actually runs the user service program.
   */
  private class UserService extends AbstractIdleService implements Closeable {
    private final CompletableFuture<ProgramController> controllerFuture;
    private final CompletableFuture<ProgramController.State> programCompletion;
    private final CConfiguration cConf;
    private final ProgramRunner programRunner;
    private final ArtifactFinder artifactFinder;
    private final StructuredTableAdmin tableAdmin;
    private final StructuredTableRegistry tableRegistry;
    private final ApplicationSpecification appSpec;
    private final ProgramOptions programOptions;
    private final List<StructuredTableSpecification> tableSpecifications;
    private final ProgramRunId programRunId;
    private final String bindHost;
    private final long maxStopSeconds;
    private File pluginsDir;
    private Program program;

    private UserService(CConfiguration cConf, ArtifactFinder artifactFinder, ProgramRunner programRunner,
                        ProgramStateWriter programStateWriter, ApplicationSpecification appSpec,
                        ProgramOptions programOptions,
                        List<StructuredTableSpecification> tableSpecifications,
                        ProgramRunId programRunId, ServiceOptions serviceOptions,
                        StructuredTableAdmin tableAdmin,
                        StructuredTableRegistry tableRegistry) {
      this.controllerFuture = new CompletableFuture<>();
      this.controllerFuture.thenAcceptAsync(
        c -> c.addListener(new StateChangeListener(programRunId, serviceOptions.getTwillRunId(), programStateWriter),
                           Threads.SAME_THREAD_EXECUTOR));
      this.programCompletion = new CompletableFuture<>();
      this.cConf = cConf;
      this.programRunner = programRunner;
      this.artifactFinder = artifactFinder;
      this.appSpec = appSpec;
      this.programOptions = programOptions;
      this.tableAdmin = tableAdmin;
      this.tableRegistry = tableRegistry;
      this.tableSpecifications = tableSpecifications;
      this.bindHost = serviceOptions.getBindAddress();
      this.programRunId = programRunId;
      this.maxStopSeconds = cConf.getLong(Constants.AppFabric.PROGRAM_MAX_STOP_SECONDS, 60);
    }

    @Override
    protected void startUp() throws Exception {
      // fetch the app artifact
      NamespaceId programNamespace = programRunId.getNamespaceId();
      Set<ArtifactId> pluginArtifacts = new HashSet<>();
      for (Plugin plugin : appSpec.getPlugins().values()) {
        pluginArtifacts.add(convert(programNamespace, plugin.getArtifactId()));
      }

      // Create system tables
      if (!tableSpecifications.isEmpty()) {
        tableRegistry.initialize();
      }
      for (StructuredTableSpecification spec : tableSpecifications) {
        LOG.info("wyzhang: creating {}", spec.toString());
        tableAdmin.create(spec);
      }

      // need to set the plugins directory in system args, as well as the host to bind to
      File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));
      pluginsDir = new File(tmpDir, "plugins");
      Map<String, String> systemArgs = new HashMap<>(programOptions.getArguments().asMap());
      systemArgs.put(ProgramOptionConstants.PLUGIN_DIR, pluginsDir.getAbsolutePath());
      systemArgs.put(ProgramOptionConstants.HOST, bindHost);
      // TODO: (CDAP-15018) figure out scaling
      systemArgs.put(ProgramOptionConstants.INSTANCE_ID, "0");
      systemArgs.put(ProgramOptionConstants.INSTANCES, "1");
      ProgramOptions updatedOptions = new SimpleProgramOptions(programRunId.getParent(), new BasicArguments(systemArgs),
                                                               programOptions.getUserArguments());

      File programDir = new File(tmpDir, "program");
      DirUtils.mkdirs(programDir);
      File programJarFile = new File(programDir, "program.jar");
      Location programJarLocation =
        artifactFinder.getArtifactLocation(convert(programNamespace, appSpec.getArtifactId()));
      Locations.linkOrCopy(programJarLocation, programJarFile);
      // Create the Program instance
      programJarLocation = Locations.toLocation(programJarFile);
      // Unpack the JAR file
      BundleJarUtil.unJar(programJarLocation, programDir);
      program = Programs.create(cConf, programRunner,
                                new ProgramDescriptor(programRunId.getParent(), appSpec), programJarLocation,
                                programDir);

      DirUtils.mkdirs(pluginsDir);
      Map<ArtifactId, Location> pluginLocations = artifactFinder.getArtifactLocations(pluginArtifacts);
      for (Map.Entry<ArtifactId, Location> pluginEntry : pluginLocations.entrySet()) {
        io.cdap.cdap.api.artifact.ArtifactId artifactId = pluginEntry.getKey().toApiArtifactId();
        Location location = pluginEntry.getValue();
        File destFile = new File(pluginsDir, String.format("%s-%s-%s.jar", artifactId.getScope().name(),
                                                           artifactId.getName(), artifactId.getVersion()));
        Locations.linkOrCopy(location, destFile);
      }
      LOG.info("Starting program run {}", programRunId);

      // Start the program.
      ProgramController controller = programRunner.run(program, updatedOptions);
      controller.addListener(new AbstractListener() {
        @Override
        public void init(ProgramController.State currentState, @Nullable Throwable cause) {
          switch (currentState) {
            case ALIVE:
              alive();
              break;
            case COMPLETED:
              completed();
              break;
            case KILLED:
              killed();
              break;
            case ERROR:
              error(cause);
              break;
          }
        }

        @Override
        public void alive() {
          controllerFuture.complete(controller);
        }

        @Override
        public void completed() {
          controllerFuture.complete(controller);
          programCompletion.complete(ProgramController.State.COMPLETED);
        }

        @Override
        public void killed() {
          controllerFuture.complete(controller);
          programCompletion.complete(ProgramController.State.KILLED);
        }

        @Override
        public void error(Throwable cause) {
          controllerFuture.complete(controller);
          programCompletion.completeExceptionally(cause);
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }

    @Override
    protected void shutDown() throws Exception {
      try {
        // If the program is not completed, get the controller and call stop
        CompletableFuture<ProgramController.State> programCompletion = this.programCompletion;

        // If there is no program completion future or it is already done, simply return as there is nothing to stop.
        if (programCompletion == null || programCompletion.isDone()) {
          return;
        }

        // Don't block forever to get the controller. The controller future might be empty if there is
        // systematic failure such that program runner is not reacting correctly
        ProgramController controller = controllerFuture.get(5, TimeUnit.SECONDS);

        LOG.info("Stopping runnable: {}.", programRunId.getProgram());

        // Give some time for the program to stop
        controller.stop().get(maxStopSeconds, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void close() throws IOException {
      LOG.info("Program run {} completed. Releasing resources.", programRunId);
      IOException failure = null;
      if (program != null) {
        try {
          program.close();
        } catch (IOException e) {
          failure = e;
        }
      }
      if (programRunner != null && programRunner instanceof Closeable) {
        try {
          ((Closeable) programRunner).close();
        } catch (IOException e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (pluginsDir != null) {
        try {
          DirUtils.deleteDirectoryContents(pluginsDir);
        } catch (IOException e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }
  }
}
