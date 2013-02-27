package com.continuuity.test;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.Id;
import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.FlowDescriptor;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.app.services.RunIdentifier;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.discovery.DiscoveryServiceClient;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.BufferFileInputStream;
import com.continuuity.internal.test.DefaultProcedureClient;
import com.continuuity.internal.test.DefaultStreamWriter;
import com.continuuity.internal.test.ProcedureClientFactory;
import com.continuuity.internal.test.StreamWriterFactory;
import com.continuuity.internal.test.bytecode.FlowletRewriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 *
 */
public class AppFabricTestBase {

  private static File testAppDir;
  private static File tmpDir;
  private static AppFabricService.Iface appFabricServer;
  private static LocationFactory locationFactory;
  private static Injector injector;
  private static DiscoveryServiceClient discoveryServiceClient;

  protected ApplicationManager deployApplication(Application application) {
    Preconditions.checkNotNull(application, "Application cannot be null.");

    final String accountId = "developer";
    Class<? extends Application> applicationClz = application.getClass();
    ApplicationSpecification appSpec = application.configure();
    final String applicationId = appSpec.getName();

    // Create the deployment jar
    Location deployedJar = locationFactory.create(createDeploymentJar(applicationClz, appSpec).getAbsolutePath());

    try {
      // Call init to get a session identifier - yes, the name needs to be changed.
      final AuthToken token = new AuthToken("appFabricTest");
      ResourceIdentifier id = appFabricServer.init(
        token, new ResourceInfo(accountId, "", applicationId, 0, System.currentTimeMillis()));

      // Upload the jar file to remote location.
      BufferFileInputStream is = new BufferFileInputStream(deployedJar.getInputStream(), 100*1024);
      try {
        byte[] chunk = is.read();
        while (chunk.length > 0) {
          appFabricServer.chunk(token, id, ByteBuffer.wrap(chunk));
          chunk = is.read();
          DeploymentStatus status = appFabricServer.dstatus(token, id);
          Preconditions.checkState(status.getOverall() == 2, "Fail to deploy app.");
        }
      } finally {
        is.close();
      }

      appFabricServer.deploy(token, id);
      int status = appFabricServer.dstatus(token, id).getOverall();
      while(status == 3) {
        status = appFabricServer.dstatus(token, id).getOverall();
        TimeUnit.MILLISECONDS.sleep(100);
      }
      Preconditions.checkState(status == 5, "Fail to deploy app.");

      return new ApplicationManager() {
        @Override
        public FlowManager startFlow(String flowName) {
          try {
            final FlowIdentifier flowId = new FlowIdentifier(accountId, applicationId, flowName, 0);
            RunIdentifier runId = appFabricServer.start(token, new FlowDescriptor(flowId, ImmutableList.<String>of()));

            return new FlowManager() {
              @Override
              public void setFlowletInstances(String flowletName, int instances) {
                try {
                  appFabricServer.setInstances(token, flowId, flowletName, (short)instances);
                } catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }

              @Override
              public void stop() {
                try {
                  appFabricServer.stop(token, flowId);
                } catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            };
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public ProcedureManager startProcedure(final String procedureName) {
          try {
            final FlowIdentifier procedureId = new FlowIdentifier(accountId, applicationId, procedureName, 0);
            procedureId.setType(EntityType.QUERY);
            RunIdentifier runId = appFabricServer.start(token, new FlowDescriptor(procedureId, ImmutableList.<String>of()));

            return new ProcedureManager() {

              @Override
              public void stop() {
                try {
                  appFabricServer.stop(token, procedureId);
                } catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }

              @Override
              public ProcedureClient getClient() {
                return injector.getInstance(ProcedureClientFactory.class)
                  .create(accountId, applicationId, procedureName);
              }
            };
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }

        @Override
        public StreamWriter getStreamWriter(String streamName) {
          QueueName queueName = QueueName.fromStream(Id.Account.from(accountId), streamName);
          return injector.getInstance(StreamWriterFactory.class).create(queueName, accountId, applicationId);
        }
      };

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  @BeforeClass
  public static final void init() {
    testAppDir = Files.createTempDir();

    File outputDir = new File(testAppDir, "app");
    tmpDir = new File(testAppDir, "tmp");

    outputDir.mkdirs();
    tmpDir.mkdirs();

    CConfiguration configuration = CConfiguration.create();
    configuration.set("app.output.dir", outputDir.getAbsolutePath());
    configuration.set("app.tmp.dir", tmpDir.getAbsolutePath());

    injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                    new BigMamaModule(configuration),
                                    new AbstractModule() {
                                      @Override
                                      protected void configure() {
                                        install(new FactoryModuleBuilder()
                                                  .implement(StreamWriter.class, DefaultStreamWriter.class)
                                                  .build(StreamWriterFactory.class));
                                        install(new FactoryModuleBuilder()
                                                  .implement(ProcedureClient.class, DefaultProcedureClient.class)
                                                  .build(ProcedureClientFactory.class));
                                      }
                                    });

    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    discoveryServiceClient.startAndWait();
    injector.getInstance(DiscoveryService.class).startAndWait();
    appFabricServer = injector.getInstance(AppFabricService.Iface.class);
    locationFactory = injector.getInstance(LocationFactory.class);
  }

  @AfterClass
  public static final void finish() {
    cleanDir(testAppDir);
  }

  private static void cleanDir(File dir) {
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      if (file.isFile()) {
        file.delete();
      } else {
        cleanDir(file);
      }
    }
  }

  /**
   * Creates a jar of a directory.
   * @param dir
   * @param manifest
   * @param outputFile
   */
  private static File jarDir(File dir, File relativeBase, Manifest manifest,
                             File outputFile, ApplicationSpecification appSpec) throws IOException {
    System.out.println(outputFile);

    JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(outputFile), manifest);
    Queue<File> queue = Lists.newLinkedList();
    Collections.addAll(queue, dir.listFiles());

    Set<String> flowletClassNames = Sets.newHashSet();
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      for (FlowletDefinition flowletDef : flowSpec.getFlowlets().values()) {
        flowletClassNames.add(flowletDef.getFlowletSpec().getClassName());
      }
    }

    FlowletRewriter rewriter = new FlowletRewriter(flowletClassNames);

    URI basePath = relativeBase.toURI();
    while (!queue.isEmpty()) {
      File file = queue.remove();
      jarOut.putNextEntry(new JarEntry(basePath.relativize(file.toURI()).toString()));

      if (file.isFile()) {
        InputStream is = new FileInputStream(file);
        try {
          jarOut.write(rewriter.rewrite(ByteStreams.toByteArray(is)));
        } finally {
          is.close();
        }
      } else {
        Collections.addAll(queue, file.listFiles());
      }
      jarOut.closeEntry();
    }

    jarOut.close();

    return outputFile;
  }

  private File createDeploymentJar(Class<?> clz, ApplicationSpecification appSpec) {
    // Creates Manifest
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, clz.getName());

    ClassLoader loader = clz.getClassLoader();
    Preconditions.checkArgument(loader != null, "Cannot get ClassLoader for class " + clz);
    String classFile = clz.getName().replace('.', '/') + ".class";

    try {
      for(Enumeration<URL> itr = loader.getResources(classFile); itr.hasMoreElements(); ) {
        URI uri = itr.nextElement().toURI();
        if (uri.getScheme().equals("file")) {
          File baseDir = new File(uri).getParentFile();

          String packagePath = clz.getPackage().getName().replace('.', '/');
          String basePath = baseDir.getAbsolutePath();
          File relativeBase = new File(basePath.substring(0, basePath.length() - packagePath.length()));

          File jarFile = File.createTempFile(
                            String.format("%s-%d", clz.getSimpleName(), System.currentTimeMillis()),
                            ".jar",
                            tmpDir);
          return jarDir(baseDir, relativeBase, manifest, jarFile, appSpec);
        } else if (uri.getScheme().equals("jar")) {
          return new File(uri.getPath());
        }
      }
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
    return null;
  }
}
