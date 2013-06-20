package com.continuuity.performance.runner;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.GeneratorFlowlet;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.guice.LocationRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.metrics2.thrift.Counter;
import com.continuuity.metrics2.thrift.CounterRequest;
import com.continuuity.metrics2.thrift.FlowArgument;
import com.continuuity.metrics2.thrift.MetricsFrontendService;
import com.continuuity.performance.application.BenchmarkManagerFactory;
import com.continuuity.performance.application.BenchmarkRuntimeMetrics;
import com.continuuity.performance.application.BenchmarkStreamWriterFactory;
import com.continuuity.performance.application.DefaultBenchmarkManager;
import com.continuuity.performance.application.MensaMetricsReporter;
import com.continuuity.performance.gateway.stream.MultiThreadedStreamWriter;
import com.continuuity.pipeline.PipelineFactory;
import com.continuuity.test.app.ApplicationManager;
import com.continuuity.test.app.DefaultProcedureClient;
import com.continuuity.test.app.FlowManager;
import com.continuuity.test.app.ProcedureClient;
import com.continuuity.test.app.ProcedureClientFactory;
import com.continuuity.test.app.StreamWriter;
import com.continuuity.test.app.TestHelper;
import com.continuuity.test.app.internal.bytecode.FlowletRewriter;
import com.continuuity.test.app.internal.bytecode.ProcedureRewriter;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.hsqldb.lib.StringUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.internal.runners.model.ReflectiveCallable;
import org.junit.internal.runners.statements.Fail;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.TestClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Runner for performance tests. This class is using lots of classes and code from JUnit framework.
 */
public final class PerformanceTestRunner {

  TestHelper th;
  private static final Logger LOG = LoggerFactory.getLogger(PerformanceTestRunner.class);
  private static File tmpDir;
  private static AppFabricService.Iface appFabricServer;
  private static LocationFactory locationFactory;
  private static Injector injector;

  private TestClass testClass;
  private final CConfiguration config;
  private String accountId = "developer";

  private ApplicationManager appManager;

  private PerformanceTestRunner() {
    config = CConfiguration.create();
  }

  public String getAccountId() {
    return accountId;
  }

  private boolean parseOptions(String[] args) throws ClassNotFoundException {
    testClass = new TestClass(Class.forName(args[0]));

    if (args.length == 1) {
      return true;
    }

    int start;
    if (args[1].endsWith(".xml")) {
      config.addResource(args[1]);
      start = 2;
    } else {
      start = 1;
    }

    LOG.debug("Parsing command line options...");
    for (int i = start; i < args.length; i++) {
      if (i + 1 < args.length) {
        String key = args[i];
        String value = args[++i];
        if ("accountid".equalsIgnoreCase(key)) {
          accountId = value;
        } else {
          if (key.startsWith("perf.")) {
            config.set(key, value);
          } else {
            config.set("perf." + key, value);
          }

        }
      } else {
        throw new RuntimeException("<key> must have an argument <value>.");
      }
    }
    return true;
  }

  public static void main(String[] args) throws Throwable {
    PerformanceTestRunner runner = new PerformanceTestRunner();
    boolean ok = runner.parseOptions(args);
    if (ok) {
      runner.runTest();
    }
  }

  private void runTest() throws Throwable {
    List<Throwable> errors = new ArrayList<Throwable>();

    try {

      beforeClass();

      for (FrameworkMethod eachBefore : testClass.getAnnotatedMethods(BeforeClass.class)) {
        try {
          eachBefore.invokeExplosively(null);
        } catch (Throwable e) {
          errors.add(e);
        }
      }

      Object testClassConstructor = getTarget();
      for (FrameworkMethod eachTest : testClass.getAnnotatedMethods(PerformanceTest.class)) {
        beforeMethod(eachTest);
        for (FrameworkMethod eachBefore : testClass.getAnnotatedMethods(Before.class)) {
          eachBefore.invokeExplosively(testClassConstructor);
        }
        eachTest.invokeExplosively(testClassConstructor);
        for (FrameworkMethod eachAfter : testClass.getAnnotatedMethods(After.class)) {
          try {
            eachAfter.invokeExplosively(testClassConstructor);
          } catch (Throwable e) {
            errors.add(e);
          }
        }
        afterMethod(eachTest);
      }

      for (FrameworkMethod eachAfter : testClass.getAnnotatedMethods(AfterClass.class)) {
        try {
          eachAfter.invokeExplosively(null);
        } catch (Throwable e) {
          errors.add(e);
        }
      }
      MultipleFailureException.assertEmpty(errors);
    } finally {
      afterClass();
    }
  }

  private List<FrameworkMethod> getAnnotatedMethods(Class<? extends Annotation> annotationClass) {
     return testClass.getAnnotatedMethods(annotationClass);
  }

  private Annotation[] getClassAnnotations() {
    return testClass.getAnnotations();
  }

  private Class<? extends Application>[] getApplications(TestClass testClass) {
    RunWithApps runWithApps = testClass.getJavaClass().getAnnotation(RunWithApps.class);
    if (runWithApps == null) {
      return null;
    }
    return runWithApps.value();
  }

  private Class<? extends Application>[] getApplications(FrameworkMethod testMethod) {
    RunWithApps runWithApps = testMethod.getAnnotation(RunWithApps.class);
    if (runWithApps == null) {
      return null;
    }
    return runWithApps.value();
  }

  private Class<? extends Application>[] getApplications2() {
    Annotation[] annos = getClassAnnotations();
    for (Annotation each : annos) {
      if (each.annotationType() == RunWithApps.class) {
        RunWithApps runWithApps = (RunWithApps) each;
        return runWithApps.value();
      }
    }
    return null;
  }

  private Object getTarget() {
    Object test;
    try {
      test = new ReflectiveCallable() {
        @Override
        protected Object runReflectiveCall() throws Throwable {
          return testClass.getOnlyConstructor().newInstance();
        }
      }.run();
      return test;
    } catch (Throwable e) {
      return new Fail(e);
    }
  }

  public ApplicationManager deployApplication(Class<? extends Application> applicationClz) {
    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    try {

      ApplicationSpecification appSpec = applicationClz.newInstance().configure();

      Location deployedJar = TestHelper.deployApplication(appFabricServer, locationFactory, new Id.Account(accountId),
                                                          TestHelper.DUMMY_AUTH_TOKEN, "", appSpec.getName(),
                                                          applicationClz);

      return
        injector.getInstance(BenchmarkManagerFactory.class).create(TestHelper.DUMMY_AUTH_TOKEN,
                                                                   accountId, appSpec.getName(),
                                                                     appFabricServer, deployedJar, appSpec);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  protected void clearAppFabric() {
    try {
      appFabricServer.reset(TestHelper.DUMMY_AUTH_TOKEN, accountId);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }  }

  private void init(CConfiguration configuration) {
    LOG.debug("Initializing Continuuity Reactor for a performance test.");
    File testAppDir = Files.createTempDir();

    File outputDir = new File(testAppDir, "app");
    tmpDir = new File(testAppDir, "tmp");
    outputDir.mkdirs();
    tmpDir.mkdirs();

    configuration.set("app.output.dir", outputDir.getAbsolutePath());
    configuration.set("app.tmp.dir", tmpDir.getAbsolutePath());

    try {
      LOG.debug("Connecting with remote AppFabric server");
      appFabricServer = getAppFabricClient();
    } catch (TTransportException e) {
      LOG.error("Error when trying to open connection with remote AppFabric.");
      Throwables.propagate(e);
    }

    Module dataFabricModule;
    if (configuration.get("perf.datafabric.mode") != null
      && configuration.get("perf.datafabric.mode").equals("distributed")) {
      dataFabricModule = new DataFabricModules().getDistributedModules();
    } else {
      dataFabricModule = new DataFabricModules().getSingleNodeModules();
    }

    injector = Guice
      .createInjector(dataFabricModule,
                      new ConfigModule(configuration),
                      new IOModule(),
                      new LocationRuntimeModule().getInMemoryModules(),
                      new DiscoveryRuntimeModule().getInMemoryModules(),
                      new ProgramRunnerRuntimeModule().getInMemoryModules(), new AbstractModule() {
                        @Override
                        protected void configure() {
                          install(new FactoryModuleBuilder().implement(ApplicationManager.class,
                                                                       DefaultBenchmarkManager.class).build
                            (BenchmarkManagerFactory.class));
                          install(new FactoryModuleBuilder().implement(StreamWriter.class,
                                                                       MultiThreadedStreamWriter.class).build
                            (BenchmarkStreamWriterFactory.class));
                          install(new FactoryModuleBuilder().implement(ProcedureClient.class,
                                                                       DefaultProcedureClient.class).build
                            (ProcedureClientFactory.class));
                        }
                      }, new Module() {
                        @Override
                        public void configure(Binder binder) {
                          binder.bind(new TypeLiteral<PipelineFactory<?>>() {
                          }).to(new TypeLiteral<SynchronousPipelineFactory<?>>() {
                          });
                          binder.bind(ManagerFactory.class).to(SyncManagerFactory.class);

                          binder.bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
                          binder.bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
                          binder.bind(AppFabricService.Iface.class).toInstance(appFabricServer);
                          binder.bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
                          binder.bind(StoreFactory.class).to(MDSStoreFactory.class);
                        }
                      }
      );

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

  private static AppFabricService.Client getAppFabricClient() throws TTransportException  {
    CConfiguration config = CConfiguration.create();
    String host = config.get(Constants.CFG_APP_FABRIC_SERVER_ADDRESS,
                             Constants.DEFAULT_APP_FABRIC_SERVER_ADDRESS);
    int port = config.getInt(Constants.CFG_APP_FABRIC_SERVER_PORT,
                             Constants.DEFAULT_APP_FABRIC_SERVER_PORT);
    return new AppFabricService.Client(getThriftProtocol(config.get(Constants.CFG_APP_FABRIC_SERVER_ADDRESS,
                                                                    Constants.DEFAULT_APP_FABRIC_SERVER_ADDRESS),
                                                         config.getInt(Constants.CFG_APP_FABRIC_SERVER_PORT,
                                                                       Constants.DEFAULT_APP_FABRIC_SERVER_PORT)));
  }

  private static TProtocol getThriftProtocol(String serviceHost, int servicePort) throws TTransportException {
    TTransport transport = new TFramedTransport(new TSocket(serviceHost, servicePort));
    try {
      transport.open();
    } catch (TTransportException e) {
      String message = String.format("Unable to connect to thrift service at %s:%d. Reason: %s", serviceHost,
                                     servicePort, e.getMessage());
      LOG.error(message);
      throw e;
    }
    return new TBinaryProtocol(transport);
  }

  private void beforeClass() throws ClassNotFoundException {
    init(config);
    Context runManager = Context.getInstance(this);
    Class<? extends Application>[] apps = getApplications(testClass);
    if (apps != null && apps.length != 0) {
      clearAppFabric();
      for (Class<? extends Application> each : apps) {
        appManager = deployApplication(each);
        runManager.addApplicationManager(each.getSimpleName(), appManager);
      }
    }
    if ("true".equalsIgnoreCase(config.get("perf.reporter.enabled"))) {
      String metrics = config.get("perf.report.metrics");
      if (StringUtils.isNotEmpty(metrics)) {
        List<String> metricList = ImmutableList.copyOf(metrics.replace(" ", "").split(","));
        String tags = "";
        int interval = 10;
        if (StringUtils.isNotEmpty(config.get("perf.report.interval"))) {
          interval = Integer.valueOf(config.get("perf.report.interval"));
        }
        Context.report(metricList, tags, interval);
      }
    }
  }

  private void beforeMethod(FrameworkMethod testMethod) {
    Class<? extends Application>[] apps = getApplications(testMethod);
    if (apps != null && apps.length != 0) {
      clearAppFabric();
      for (Class<? extends Application> each : apps) {
        appManager = deployApplication(each);
        Context.getInstance().addApplicationManager(each.getSimpleName(), appManager);
      }
    }
  }

  private void afterMethod(FrameworkMethod testMethod) {
    Class<? extends Application>[] apps = getApplications(testMethod);
    if (apps != null && apps.length != 0) {
      Context.stopAll();
      clearAppFabric();
    }
  }

  private void afterClass() {
    Context.stopAll();
  }

  /**
   * Context for managing components of a bytecode test.
   */
  public static final class Context {
    private static Context one;

    private final PerformanceTestRunner runner;
    private final Map<String, ApplicationManager> appManagers;
    private final Set<MultiThreadedStreamWriter> streamWriters;
    private final List<MensaMetricsReporter> mensaReporters;

    protected static Context getInstance(PerformanceTestRunner runner) {
      if (one == null) {
        one = new Context(runner);
      }
      return one;
    }

    private static Context getInstance() {
      if (one == null) {
        throw new NullPointerException("PerformanceTestRunner has not instantiated Context.");
      }
      return one;
    }

    private Context(PerformanceTestRunner runner) {
      this.runner = runner;
      appManagers = new HashMap<String, ApplicationManager>();
      streamWriters = new HashSet<MultiThreadedStreamWriter>();
      mensaReporters = new ArrayList<MensaMetricsReporter>();
    }

    protected void addApplicationManager(String appName, ApplicationManager manager) {
      appManagers.put(appName, manager);
    }

    public static ApplicationManager getApplicationManager(String appName) {
      return getInstance().appManagers.get(appName);
    }

    public final CConfiguration getConfiguration() {
      return runner.config;
    }

    public static FlowManager startFlow(String appName, String flowName) {
      return getInstance().getApplicationManager(appName).startFlow(flowName);
    }

    public static BenchmarkRuntimeMetrics getFlowletMetrics(String appName, String flowName, String flowletName) {
      return BenchmarkRuntimeStats.getFlowletMetrics(getInstance().runner.accountId, appName, flowName, flowletName);
    }

    public static StreamWriter getStreamWriter(String appName, String streamName) {
      StreamWriter streamWriter = getInstance().getApplicationManager(appName).getStreamWriter(streamName);
      if (streamWriter instanceof  MultiThreadedStreamWriter) {
        getInstance().streamWriters.add((MultiThreadedStreamWriter) streamWriter);
      }
      return streamWriter;
    }

    public static void report(List<String> counters, String tags, int interval) {
      MensaMetricsReporter reporter =
        new MensaMetricsReporter(getInstance().getConfiguration(), counters, tags, interval);
      getInstance().mensaReporters.add(reporter);
    }

    public static void reportNow(String counter, double value) {
      if (!getInstance().mensaReporters.isEmpty()) {
        getInstance().mensaReporters.get(0).reportNow(counter, value);
      }
    }

    public static void reportNow(String counter) {
      if (!getInstance().mensaReporters.isEmpty()) {
        getInstance().mensaReporters.get(0).reportNow(counter);
      }
    }

    public static void stopAll() {
      for (MultiThreadedStreamWriter streamWriter : getInstance().streamWriters) {
        streamWriter.shutdown();
      }
      for (ApplicationManager manager : getInstance().appManagers.values()) {
        manager.stopAll();
      }
      for (MensaMetricsReporter reporter : getInstance().mensaReporters) {
        reporter.shutdown();
      }
    }
  }
  /**
   * Runtime statistics of an application during a bytecode test.
   */
  public static final class BenchmarkRuntimeStats {

    private static MetricsFrontendService.Client metricsClient = getMetricsClient();

    public static BenchmarkRuntimeMetrics getFlowletMetrics(final String accountId, final String applicationId, final String flowId,
                                                            final String flowletId) {
      final String inputName = String.format("%s.tuples.read.count", flowletId);
      final String processedName = String.format("%s.processed.count", flowletId);

      return new BenchmarkRuntimeMetrics() {
        @Override
        public long getInput() {
          Double input = getCounters(accountId, applicationId, flowId, flowletId).get(inputName);
          if (input == null) {
            return 0L;
          } else {
            return input.longValue();
          }
        }

        @Override
        public long getProcessed() {
          Double processed = getCounters(accountId, applicationId, flowId, flowletId).get(processedName);
          if (processed == null) {
            return 0L;
          } else {
            return processed.longValue();
          }
        }

        @Override
        public void waitForinput(long count, long timeout, TimeUnit timeoutUnit)
          throws TimeoutException, InterruptedException {
          waitFor(inputName, count, timeout, timeoutUnit);
        }

        @Override
        public void waitForProcessed(long count, long timeout, TimeUnit timeoutUnit)
          throws TimeoutException, InterruptedException {
          waitFor(processedName, count, timeout, timeoutUnit);
        }

        private void waitFor(String name, long count, long timeout, TimeUnit timeoutUnit)
          throws TimeoutException, InterruptedException {
          Double value = getCounters(accountId, applicationId, flowId, flowletId).get(name);
          while (timeout > 0 && (value == null || value.longValue() < count)) {
            timeoutUnit.sleep(1);
            value = getCounters(accountId, applicationId, flowId, flowletId).get(name);
            timeout--;
          }
          if (timeout == 0 && (value == null || value.longValue() < count)) {
            throw new TimeoutException("Time limit reached.");
          }
        }

        @Override
        public String toString() {
          return String.format("%s; input=%d, processed=%d, exception=%d", flowletId, getInput(), getProcessed());
        }
      };
    }

    public static void waitForCounter(String accountId, String applicationId, String flowName, String flowletName, String counterName,
                                      long count, long timeout, TimeUnit timeoutUnit)
      throws TimeoutException, InterruptedException {
      Counter c = getCounter(accountId, applicationId, flowName, flowletName, counterName);
      if (c == null) {
        throw new RuntimeException("No counter with name " + counterName + " found for application " + applicationId
                                     + " ,flow " + flowName + " and flowlet " + flowletName + ".");
      }
      double value = c.getValue();
      while (timeout > 0 && (value < count)) {
        timeoutUnit.sleep(1);
        value = getCounter(counterName).getValue();
        timeout--;
      }
      if (timeout == 0 && (value < count)) {
        throw new TimeoutException("Time limit reached.");
      }
    }

    public static void waitForCounter(String counterName, long count, long timeout, TimeUnit timeoutUnit)
      throws TimeoutException,
      InterruptedException {
      Counter c = getCounter(counterName);
      if (c == null) {
        throw new RuntimeException("No counter with name " + counterName + " found.");
      }
      double value = c.getValue();
      while (timeout > 0 && (value < count)) {
        timeoutUnit.sleep(1);
        value = getCounter(counterName).getValue();
        timeout--;
      }
      if (timeout == 0 && (value < count)) {
        throw new TimeoutException("Time limit reached.");
      }
    }

    public static Counter getCounter(String accountId, String applicationId, String flowName, String flowletName, String counterName) {
      FlowArgument arg = new FlowArgument(accountId, applicationId, flowName);
      try {
        List<Counter> counters = metricsClient.getCounters(new CounterRequest(arg));
        for (Counter counter : counters) {
          if (counter.getQualifier().equals(flowletName) && counter.getName().equals(counterName)) {
            return counter;
          }
        }
        return null;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public static Counter getCounter(String counterName) {
      FlowArgument arg = new FlowArgument("-", "-", "-");
      arg.setFlowletId("-");
      CounterRequest req = new CounterRequest(arg);
      req.setName(ImmutableList.of(counterName));
      try {
        List<Counter> counters = metricsClient.getCounters(req);
        if (counters != null && counters.size() != 0) {
          return counters.get(0);
        }
        return null;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public static Map<String, Double> getCounters(String accountId, String applicationId, String flowName) {
      return getCounters(accountId, applicationId, flowName, null);
    }

    public static Map<String, Double> getCounters(String accountId, String applicationId, String flowName, String flowletName) {
      FlowArgument arg = new FlowArgument(accountId, applicationId, flowName);
      try {
        List<Counter> counters = metricsClient.getCounters(new CounterRequest(arg));
        Map<String, Double> counterMap = new HashMap<String, Double>(counters.size());
        if (StringUtil.isEmpty(flowletName)) {
          for (Counter counter : counters) {
            counterMap.put(counter.getQualifier() + "." + counter.getName(), counter.getValue());
          }
        } else {
          for (Counter counter : counters) {
            if (counter.getQualifier().equals(flowletName)) {
              counterMap.put(counter.getQualifier() + "." + counter.getName(), counter.getValue());
            }
          }
        }
        return counterMap;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static MetricsFrontendService.Client getMetricsClient() {
      CConfiguration config = CConfiguration.create();
      try {
        return
          new MetricsFrontendService.Client(
            getThriftProtocol(config.get(Constants.CFG_METRICS_FRONTEND_SERVER_ADDRESS,
                                         Constants.DEFAULT_OVERLORD_SERVER_ADDRESS),
                              config.getInt(Constants.CFG_METRICS_FRONTEND_SERVER_PORT,
                                            Constants.DEFAULT_METRICS_FRONTEND_SERVER_PORT)));
      } catch (TTransportException e) {
        Throwables.propagate(e);
      }
      return null;
    }

    private static TProtocol getThriftProtocol(String serviceHost, int servicePort) throws TTransportException {
      TTransport transport = new TFramedTransport(new TSocket(serviceHost, servicePort));
      transport.open();
      //now try to connect the thrift client
      return new TBinaryProtocol(transport);
    }
  }
}
