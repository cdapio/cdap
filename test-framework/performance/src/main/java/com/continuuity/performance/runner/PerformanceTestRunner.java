/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.runner;

import com.continuuity.api.Application;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.handlers.AppFabricHttpHandler;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.LocalManager;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.store.MDTBasedStoreFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.performance.application.BenchmarkManagerFactory;
import com.continuuity.performance.application.BenchmarkStreamWriterFactory;
import com.continuuity.performance.application.DefaultBenchmarkManager;
import com.continuuity.performance.application.MensaMetricsReporter;
import com.continuuity.performance.gateway.stream.MultiThreadedStreamWriter;
import com.continuuity.pipeline.PipelineFactory;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.ProcedureClient;
import com.continuuity.test.StreamWriter;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.continuuity.test.internal.DefaultProcedureClient;
import com.continuuity.test.internal.ProcedureClientFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import org.apache.commons.lang.StringUtils;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.zookeeper.ZKClientService;
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
import java.lang.annotation.Annotation;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Runner for performance tests. This class is using lots of classes and code from JUnit framework.
 */
public final class PerformanceTestRunner {

  private static final Logger LOG = LoggerFactory.getLogger(PerformanceTestRunner.class);
  private static LocationFactory locationFactory;
  private static Injector injector;
  private static String accountId = "developer";
  private static DiscoveryServiceClient discoveryServiceClient;
  private static ZKClientService zkClientService;
  private static AppFabricHttpHandler httpHandler;

  private TestClass testClass;
  private final CConfiguration config;


  private PerformanceTestRunner() {
    config = CConfiguration.create();
  }

  /**
   * Providing discovery service client that can be used to locate metrics client.
   * @return DiscoveryServiceClient
   */
  public static DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

  // Parsing the command line options.
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

  // Core method that executes all the test methods of the current performance test class.
  private void runTest() throws Throwable {
    List<Throwable> errors = new ArrayList<Throwable>();

    try {

      // execute initialization steps
      beforeClass();

      // execute all methods annotated with @BeforeClass in the current test class before running any test method.
      for (FrameworkMethod eachBefore : testClass.getAnnotatedMethods(BeforeClass.class)) {
        try {
          eachBefore.invokeExplosively(null);
        } catch (Throwable e) {
          errors.add(e);
        }
      }
      Object testClassConstructor = getTarget();

      // for each of the performance test methods annotated with @PerformanceTest execute the following sequence of
      // methods
      for (FrameworkMethod eachTest : testClass.getAnnotatedMethods(PerformanceTest.class)) {
        for (FrameworkMethod eachBefore : testClass.getAnnotatedMethods(Before.class)) {
          // execute all methods annotated with @Before right before running the current performance test method
          eachBefore.invokeExplosively(testClassConstructor);
        }
        // execute the current performance test method
        eachTest.invokeExplosively(testClassConstructor);
        for (FrameworkMethod eachAfter : testClass.getAnnotatedMethods(After.class)) {
          try {
            // execute all methods annotated with @After right after running the current performance test method
            eachAfter.invokeExplosively(testClassConstructor);
          } catch (Throwable e) {
            errors.add(e);
          }
        }
        // execute this method after running the current performance test method
        afterMethod();
      }

      // execute all methods annotated with @AfterClass after running all defined test methods
      for (FrameworkMethod eachAfter : testClass.getAnnotatedMethods(AfterClass.class)) {
        try {
          eachAfter.invokeExplosively(null);
        } catch (Throwable e) {
          errors.add(e);
        }
      }
      MultipleFailureException.assertEmpty(errors);
    } finally {
      // execute this method at the end of the overall performance test
      afterClass();
    }
  }

  // Gets methods from test class that are annotated with @annotationClass
  @SuppressWarnings("unused")
  private List<FrameworkMethod> getAnnotatedMethods(Class<? extends Annotation> annotationClass) {
    return testClass.getAnnotatedMethods(annotationClass);
  }

  // Gets all annotations of given class.
  @SuppressWarnings("unused")
  private Annotation[] getClassAnnotations() {
    return testClass.getAnnotations();
  }

  // Gets an object of TestClass by calling parameter-less constructor.
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

  // Deploys a provided Continuuity Reactor App by the name of its class.
  @SuppressWarnings(value = "unchecked")
  public static ApplicationManager deployApplication(String applicationClass) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(applicationClass),
                                "Application cannot be null or empty String.");

    try {
      return deployApplication((Class<? extends Application>) Class.forName(applicationClass));
    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }


  // Deploys a provided Continuuity Reactor App.
  public static ApplicationManager deployApplication(Class<? extends Application> applicationClz) {
    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    try {

      ApplicationSpecification appSpec =
        Specifications.from(applicationClz.newInstance().configure());

      Location deployedJar = AppFabricTestHelper.deployApplication(httpHandler, locationFactory,
                                                                   appSpec.getName(), applicationClz);

      BenchmarkManagerFactory bmf = injector.getInstance(BenchmarkManagerFactory.class);
      ApplicationManager am = bmf.create(accountId, appSpec.getName(), httpHandler,
                                         deployedJar, appSpec);
      return am;

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Wipes out all applications and data for a given account in the Reactor.
  protected void clearAppFabric() {
    try {
      AppFabricTestHelper.reset(httpHandler);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Initializes Reactor for executing a performance test.
  private void init() {
    LOG.debug("Initializing Continuuity Reactor for a performance test.");
    File testAppDir = Files.createTempDir();

    File appDir = new File(testAppDir, "app");
    File datasetDir = new File(testAppDir, "dataset");
    File tmpDir = new File(testAppDir, "tmp");
    tmpDir.deleteOnExit();
    appDir.mkdirs();
    datasetDir.mkdirs();
    tmpDir.mkdirs();

    config.set(Constants.AppFabric.OUTPUT_DIR, appDir.getAbsolutePath());
    config.set(Constants.AppFabric.TEMP_DIR, tmpDir.getAbsolutePath());
    config.set(Constants.Dataset.Manager.OUTPUT_DIR, datasetDir.getAbsolutePath());

    List<Module> modules = Lists.newArrayList();

    try {
      if (config.get("perf.reactor.mode") != null
        && config.get("perf.reactor.mode").equals("distributed")) {
        modules.add(new DataFabricModules().getDistributedModules());
        modules.add(new LocationRuntimeModule().getDistributedModules());
        modules.add(new ZKClientModule());
        modules.add(new DiscoveryRuntimeModule().getDistributedModules());
      } else {
        modules.add(new DataFabricModules().getSingleNodeModules());
        modules.add(new LocationRuntimeModule().getInMemoryModules());
        modules.add(new AbstractModule() {
          @Override
          protected void configure() {
            final String host = config.get(MetricsConstants.ConfigKeys.SERVER_ADDRESS, "localhost");
            final int port = config.getInt(MetricsConstants.ConfigKeys.SERVER_PORT, 45005);

            bind(DiscoveryServiceClient.class).toInstance(new DiscoveryServiceClient() {
              @Override
              public ServiceDiscovered discover(final String name) {
                final Discoverable serviceDiscoverable;
                if (Constants.Service.METRICS.equals(name)) {
                  serviceDiscoverable = new Discoverable() {
                    @Override
                    public String getName() {
                      return name;
                    }

                    @Override
                    public InetSocketAddress getSocketAddress() {
                      return new InetSocketAddress(host, port);
                    }
                  };
                } else {
                  serviceDiscoverable = null;
                }
                return new ServiceDiscovered() {
                  @Override
                  public String getName() {
                    return name;
                  }

                  @Override
                  public Cancellable watchChanges(ChangeListener listener, Executor executor) {
                    // The discoverable is static, hence no change to watch
                    return new Cancellable() {
                      @Override
                      public void cancel() {
                        // No-op
                      }
                    };
                  }

                  @Override
                  public boolean contains(Discoverable discoverable) {
                    return serviceDiscoverable.getName().equals(discoverable.getName())
                      && serviceDiscoverable.getSocketAddress().equals(discoverable.getSocketAddress());
                  }

                  @Override
                  public Iterator<Discoverable> iterator() {
                    return ImmutableList.of(serviceDiscoverable).iterator();
                  }
                };
              }
            });
            bind(DiscoveryService.class).toInstance(new InMemoryDiscoveryService());
          }
        });
      }

      modules.add(new ConfigModule(config));
      modules.add(new IOModule());
      modules.add(new AppFabricServiceRuntimeModule().getInMemoryModules());
      modules.add(new AbstractModule() {
        @Override
        protected void configure() {
          install(new FactoryModuleBuilder()
                    .implement(ApplicationManager.class, DefaultBenchmarkManager.class)
                    .build(BenchmarkManagerFactory.class));
          install(new FactoryModuleBuilder()
                    .implement(StreamWriter.class, MultiThreadedStreamWriter.class)
                    .build(BenchmarkStreamWriterFactory.class));
          install(new FactoryModuleBuilder()
                    .implement(ProcedureClient.class, DefaultProcedureClient.class)
                    .build(ProcedureClientFactory.class));
        }
      });
      modules.add(new AbstractModule() {
        @Override
        public void configure() {
          bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);
          install(
            new FactoryModuleBuilder()
              .implement(new TypeLiteral<Manager<Location, ApplicationWithPrograms>>() { },
                         new TypeLiteral<LocalManager<Location, ApplicationWithPrograms>>() { })
              .build(new TypeLiteral<ManagerFactory<Location, ApplicationWithPrograms>>() { })
          );

          bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
          bind(StoreFactory.class).to(MDTBasedStoreFactory.class);
        }
        @Provides
        @Named(Constants.AppFabric.SERVER_ADDRESS)
        @SuppressWarnings(value = "unused")
        public InetAddress providesHostname(CConfiguration cConf) {
          return Networks.resolve(cConf.get(Constants.AppFabric.SERVER_ADDRESS),
                                  new InetSocketAddress("localhost", 0).getAddress());
        }
      });

      injector = Guice.createInjector(modules);
    } catch (Exception e) {
      LOG.error("Failure during initial bind and injection : " + e.getMessage(), e);
      Throwables.propagate(e);
    }
    locationFactory = injector.getInstance(LocationFactory.class);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    httpHandler = injector.getInstance(AppFabricHttpHandler.class);
    try {
      zkClientService = injector.getInstance(ZKClientService.class);
    } catch (Exception e) {
      zkClientService = null;
    }
  }

  // Gets executed once before running all the test methods of the current performance test class.
  private void beforeClass() throws ClassNotFoundException {
    // initializes Reactor
    init();
    Context.getInstance(this);

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
    if (zkClientService != null) {
      zkClientService.startAndWait();
    }
  }

  private void afterMethod() {
    clearAppFabric();
  }

  private void afterClass() {
    Context.stopAll();
  }

  // Context for managing components of a performance test.
  private static final class Context {
    private static Context one;

    private final PerformanceTestRunner runner;
    private final Set<MultiThreadedStreamWriter> streamWriters;
    private final List<MensaMetricsReporter> mensaReporters;

    private static Context getInstance(PerformanceTestRunner runner) {
      if (one == null) {
        one = new Context(runner);
      }
      return one;
    }

    private static Context getInstance() {
      if (one == null) {
        throw new RuntimeException("PerformanceTestRunner has not instantiated Context.");
      }
      return one;
    }

    private Context(PerformanceTestRunner runner) {
      this.runner = runner;
      streamWriters = new HashSet<MultiThreadedStreamWriter>();
      mensaReporters = new ArrayList<MensaMetricsReporter>();
    }

    private final CConfiguration getConfiguration() {
      return runner.config;
    }

    /**
     * Add counters to list of metrics that get frequently collected and reported.
     * @param counters List of counter names to be frequently collected and reported.
     * @param tags Comma separated tags that will be added each time counter values are reported.
     * @param interval Time interval of collection and reporting.
     */
    public static void report(List<String> counters, String tags, int interval) {
      MensaMetricsReporter reporter =
        new MensaMetricsReporter(getInstance().getConfiguration(), counters, tags, interval);
      getInstance().mensaReporters.add(reporter);
    }

    /**
     * Immediately report counter and value.
     * @param counter Name of counter.
     * @param value Value of counter.
     */
    @SuppressWarnings("unused")
    public static void reportNow(String counter, double value) {
      if (!getInstance().mensaReporters.isEmpty()) {
        getInstance().mensaReporters.get(0).reportNow(counter, value);
      }
    }

    /**
     * Immediately collect value of counter and report it.
     * @param counter Name of counter.
     */
    @SuppressWarnings("unused")
    public static void reportNow(String counter) {
      if (!getInstance().mensaReporters.isEmpty()) {
        getInstance().mensaReporters.get(0).reportNow(counter);
      }
    }

    // Stopping all stream writers and metrics reporters
    private static void stopAll() {
      for (MultiThreadedStreamWriter streamWriter : getInstance().streamWriters) {
        streamWriter.shutdown();
      }
      for (MensaMetricsReporter reporter : getInstance().mensaReporters) {
        reporter.shutdown();
      }
      if (zkClientService != null) {
        zkClientService.stopAndWait();
      }
    }
  }
}
