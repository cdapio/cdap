package com.continuuity.test;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.GeneratorFlowlet;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.discovery.DiscoveryService;
import com.continuuity.discovery.DiscoveryServiceClient;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.BufferFileInputStream;
import com.continuuity.internal.test.ApplicationManagerFactory;
import com.continuuity.internal.test.DefaultApplicationManager;
import com.continuuity.internal.test.DefaultProcedureClient;
import com.continuuity.internal.test.DefaultStreamWriter;
import com.continuuity.internal.test.ProcedureClientFactory;
import com.continuuity.internal.test.StreamWriterFactory;
import com.continuuity.internal.test.bytecode.FlowletRewriter;
import com.continuuity.internal.test.bytecode.ProcedureRewriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Base class to inherit from that provides testing functionality for {@link com.continuuity.api.Application}.
 */
public class AppFabricTestBase {

  private static File testAppDir;
  private static File tmpDir;
  private static AppFabricService.Iface appFabricServer;
  private static LocationFactory locationFactory;
  private static Injector injector;
  private static DiscoveryServiceClient discoveryServiceClient;

  /**
   * Deploys an {@link com.continuuity.api.Application}. The {@link com.continuuity.api.flow.Flow Flows} and
   * {@link com.continuuity.api.procedure.Procedure Procedures} defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link ApplicationManager} to manage the deployed application.
   */
  protected ApplicationManager deployApplication(Class<? extends Application> applicationClz) {
    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    final String accountId = "developer";
    Application application = null;
    try {
      application = applicationClz.newInstance();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
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

      // Deploy the app
      appFabricServer.deploy(token, id);
      int status = appFabricServer.dstatus(token, id).getOverall();
      while(status == 3) {
        status = appFabricServer.dstatus(token, id).getOverall();
        TimeUnit.MILLISECONDS.sleep(100);
      }
      Preconditions.checkState(status == 5, "Fail to deploy app.");

      return injector.getInstance(ApplicationManagerFactory.class).create(token, accountId, applicationId,
                                                                          appFabricServer, deployedJar, appSpec);

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
                                                  .implement(ApplicationManager.class, DefaultApplicationManager.class)
                                                  .build(ApplicationManagerFactory.class));
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

  private static String pathToClassName(String path) {
    return path.replace('/', '.').substring(0, path.length() - ".class".length());
  }

  /**
   * Creates a jar of a directory and rewrite Flowlet and Procedure bytecodes to emit metrics.
   *
   * @param dir Directory root for creating jar entries.
   * @param manifest The Jar manifest content.
   * @param outputFile Location of the Jar file.
   * @param appSpec The {@link com.continuuity.api.ApplicationSpecification} of the deploying application.
   */
  private static File jarDir(File dir, File relativeBase, Manifest manifest,
                             File outputFile, ApplicationSpecification appSpec) throws IOException,
    ClassNotFoundException {

    JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(outputFile), manifest);
    Queue<File> queue = Lists.newLinkedList();
    Collections.addAll(queue, dir.listFiles());

    // Find all flowlet classes (flowlet class => flowId)
    // FIXME: Limitation now is that the same flowlet class can be used in one flow only (can have multiple names)
    Map<String, String> flowletClassNames = Maps.newHashMap();
    for (FlowSpecification flowSpec : appSpec.getFlows().values()) {
      for (FlowletDefinition flowletDef : flowSpec.getFlowlets().values()) {
        flowletClassNames.put(flowletDef.getFlowletSpec().getClassName(), flowSpec.getName());
      }
    }

    // Find all procedure classes
    Set<String> procedureClassNames = Sets.newHashSet();
    for (ProcedureSpecification procedureSpec : appSpec.getProcedures().values()) {
      procedureClassNames.add(procedureSpec.getClassName());
    }

    FlowletRewriter flowletRewriter = new FlowletRewriter(appSpec.getName(), false);
    FlowletRewriter generatorRewriter = new FlowletRewriter(appSpec.getName(), true);
    ProcedureRewriter procedureRewriter = new ProcedureRewriter(appSpec.getName());

    URI basePath = relativeBase.toURI();
    while (!queue.isEmpty()) {
      File file = queue.remove();
      String entryName = basePath.relativize(file.toURI()).toString();
      jarOut.putNextEntry(new JarEntry(entryName));

      if (file.isFile()) {
        InputStream is = new FileInputStream(file);
        try {
          byte[] bytes = ByteStreams.toByteArray(is);
          String className = pathToClassName(entryName);
          if (flowletClassNames.containsKey(className)) {
            if (GeneratorFlowlet.class.isAssignableFrom(Class.forName(className))) {
              jarOut.write(generatorRewriter.generate(bytes, flowletClassNames.get(className)));
            } else {
              jarOut.write(flowletRewriter.generate(bytes, flowletClassNames.get(className)));
            }
          } else if (procedureClassNames.contains(className)) {
            jarOut.write(procedureRewriter.generate(bytes));
          } else {
            jarOut.write(bytes);
          }
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

          Package appPackage = clz.getPackage();
          String packagePath = appPackage == null ? "" : appPackage.getName().replace('.', '/');
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
