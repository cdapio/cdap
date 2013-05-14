package com.continuuity.performance.application;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.GeneratorFlowlet;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
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
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.ProcedureClient;
import com.continuuity.test.StreamWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Base class to inherit from that provides performance testing functionality for
 * {@link com.continuuity.api.Application}.
 */
public class RemoteAppFabricTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteAppFabricTestBase.class);
  private static File tmpDir;
  private static AppFabricService.Iface appFabricService;
  private static LocationFactory locationFactory;
  private static Injector injector;


  private Class<? extends Application> getApplicationClass(String appClassName) {
    if (!appClassName.startsWith("com.continuuity")) {
      appClassName = this.getClass().getPackage().getName() + "." + appClassName;
    }
    try {
      return (Class<? extends Application>) Class.forName(appClassName);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return null;
  }

  public ApplicationManager deployApplication(String appClassName) {
    Preconditions.checkNotNull(appClassName);
    return deployApplication(getApplicationClass(appClassName));
  }

  public ApplicationManager deployApplication(Class<? extends Application> applicationClz) {
    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    final String accountId = "developer";
    Application application;
    try {
      application = applicationClz.newInstance();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    ApplicationSpecification appSpec = application.configure();
    final String applicationId = appSpec.getName();

    // Create the deployment jar
    File jarFile = createDeploymentJar(applicationClz, appSpec);
    LOG.debug("Created local deployment jar {} for application.", jarFile.getAbsolutePath());
    Location deployedJar = locationFactory.create(jarFile.getAbsolutePath());

    try {
      // Call init to get a session identifier - yes, the name needs to be changed.
      final AuthToken token = new AuthToken("appFabricTest");
      ResourceIdentifier id = appFabricService.init(
        token, new ResourceInfo(accountId, "", applicationId, 0, System.currentTimeMillis()));

      // Upload the jar file to remote location.
      BufferFileInputStream is = new BufferFileInputStream(deployedJar.getInputStream(), 100 * 1024);
      try {
        byte[] chunk = is.read();
        while (chunk.length > 0) {
          appFabricService.chunk(token, id, ByteBuffer.wrap(chunk));
          chunk = is.read();
          DeploymentStatus status = appFabricService.dstatus(token, id);
          Preconditions.checkState(status.getOverall() == 2, "Fail to deploy app.");
        }
      } finally {
        is.close();
      }

      // Deploy the app
      appFabricService.deploy(token, id);
      int status = appFabricService.dstatus(token, id).getOverall();
      while (status == 3) {
        status = appFabricService.dstatus(token, id).getOverall();
        TimeUnit.MILLISECONDS.sleep(100);
      }
      Preconditions.checkState(status == 5, "Fail to deploy app.");

      ApplicationManager mgr
        = injector.getInstance(ApplicationManagerFactory.class).create(token, accountId, applicationId,
                                                                       appFabricService, deployedJar, appSpec);
      Preconditions.checkNotNull(mgr, "Fail to deploy app.");
      LOG.debug("Suceesfully deployed jar file {} with application.", jarFile.getAbsolutePath());
      return mgr;

    } catch (Exception e) {
      LOG.error("Deployment of jar file {} with new application failed!", jarFile.getAbsolutePath());
      throw Throwables.propagate(e);
    }
  }

  public void clearAppFabric() {
    try {
      appFabricService.reset(new AuthToken("appFabricTest"), "developer");
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static final void init() {
    LOG.debug("Initializing AppFabric for performance test.");
    File testAppDir = Files.createTempDir();

    File outputDir = new File(testAppDir, "app");
    tmpDir = new File(testAppDir, "tmp");

    outputDir.mkdirs();
    tmpDir.mkdirs();

    final CConfiguration configuration = CConfiguration.create();
    configuration.set("app.output.dir", outputDir.getAbsolutePath());
    configuration.set("app.tmp.dir", tmpDir.getAbsolutePath());
    configuration.set("zk", "db101.ubench.sl");
    configuration.set("host", "db101.ubench.sl");

    try {
      LOG.debug("Connecting with remote AppFabric server");
      appFabricService = getAppFabricClient();
    } catch (TTransportException e) {
      LOG.error("Error when trying to open connection with remote AppFabric.");
      Throwables.propagate(e);
    }

    injector = Guice
      .createInjector(new DataFabricModules().getDistributedModules(),
                      new AngryMamaModule(configuration),
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
                        }},
                      new Module() {
                        @Override
                        public void configure(Binder binder) {
                           binder.bind(AppFabricService.Iface.class).toInstance(appFabricService);
                        }
                      });

    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    discoveryServiceClient.startAndWait();
    injector.getInstance(DiscoveryService.class).startAndWait();
    locationFactory = injector.getInstance(LocationFactory.class);
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
      if (loader != null) {
        Enumeration<URL> itr = loader.getResources(classFile);
        if (itr != null) {
          while (itr.hasMoreElements()) {
            URI uri = itr.nextElement().toURI();
            if (uri.getScheme().equals("file")) {
              File baseDir = new File(uri).getParentFile();

              Package appPackage = clz.getPackage();
              String packagePath = appPackage == null ? "" : appPackage.getName().replace('.', '/');
              String basePath = baseDir.getAbsolutePath();
              File relativeBase = new File(basePath.substring(0, basePath.length() - packagePath.length()));

              File jarFile = File.createTempFile(
                String.format("%s-%d", clz.getSimpleName(), System.currentTimeMillis()), ".jar", tmpDir);
              return jarDir(baseDir, relativeBase, manifest, jarFile, appSpec);
            } else if (uri.getScheme().equals("jar")) {
              String schemeSpecificPart = uri.getSchemeSpecificPart();
              String jarFilePath =
                schemeSpecificPart.substring(schemeSpecificPart.indexOf("/"), schemeSpecificPart.indexOf("!"));
              LOG.debug("jarFilePath = {}", jarFilePath);
              return new File(jarFilePath);
            }
          }
        }
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return null;
  }
  private static String pathToClassName(String path) {
    return path.replace('/', '.').substring(0, path.length() - ".class".length());
  }

  private static File jarDir(File dir, File relativeBase, Manifest manifest, File outputFile,
                             ApplicationSpecification appSpec)
    throws IOException, ClassNotFoundException {

    JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(outputFile), manifest);
    Queue<File> queue = Lists.newLinkedList();
    File[] files = dir.listFiles();
    if (files != null) {
      Collections.addAll(queue, files);
    }

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
        files = file.listFiles();
        if (files != null) {
          Collections.addAll(queue, files);
        }
      }
      jarOut.closeEntry();
    }

    jarOut.close();

    return outputFile;
  }
  private static AppFabricService.Client getAppFabricClient()
    throws TTransportException  {
    CConfiguration configuration = CConfiguration.create();
    String host = configuration.get(Constants.CFG_APP_FABRIC_SERVER_ADDRESS,
                                    Constants.DEFAULT_APP_FABRIC_SERVER_ADDRESS);
    int port = configuration.getInt(Constants.CFG_APP_FABRIC_SERVER_PORT, Constants.DEFAULT_APP_FABRIC_SERVER_PORT);

    LOG.debug("Trying to open connection with remote AppFabric server at {}:{} ", host, port);
    TTransport transport = new TFramedTransport(new TSocket(host, port));
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    return new AppFabricService.Client(protocol);
  }
}
