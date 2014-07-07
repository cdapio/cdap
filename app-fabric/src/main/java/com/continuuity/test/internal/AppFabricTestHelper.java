package com.continuuity.test.internal;

import com.continuuity.app.Id;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.RunRecord;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.common.utils.Networks;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.handlers.AppFabricHttpHandler;
import com.continuuity.http.BodyConsumer;
import com.continuuity.internal.app.BufferFileInputStream;
import com.continuuity.internal.app.deploy.ProgramTerminator;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.schedule.SchedulerService;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;


/**
 * This is helper class to make calls to AppFabricHttpHandler methods directly.
 */
public class AppFabricTestHelper {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricTestHelper.class);
  private static final Gson GSON = new Gson();
  public static final TempFolder TEMP_FOLDER = new TempFolder();
  public static CConfiguration configuration;
  private static Injector injector;

  public static Injector getInjector() {
    return getInjector(CConfiguration.create());
  }

  public static synchronized Injector getInjector(CConfiguration conf) {
    if (injector == null) {
      configuration = conf;
      configuration.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());
      configuration.set(Constants.AppFabric.OUTPUT_DIR, TEMP_FOLDER.newFolder("app").getAbsolutePath());
      configuration.set(Constants.AppFabric.TEMP_DIR, TEMP_FOLDER.newFolder("temp").getAbsolutePath());
      configuration.set(Constants.AppFabric.REST_PORT, Integer.toString(Networks.getRandomPort()));
      configuration.setBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, true);
      injector = Guice.createInjector(new AppFabricTestModule(configuration));
      injector.getInstance(InMemoryTransactionManager.class).startAndWait();
      injector.getInstance(SchedulerService.class).startAndWait();

      LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();
    }
    return injector;
  }

  public static void reset(AppFabricHttpHandler httpHandler) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/unrecoverable/reset");
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    httpHandler.resetReactor(request, responder);
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, "reset application failed");
  }

  public static void startProgram(AppFabricHttpHandler httpHandler, String appId, String flowId,
                                  String type, Map<String, String> args) {

    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/%s/%s/start", appId, type, flowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    String argString = GSON.toJson(args);
    if (argString != null) {
      request.setContent(ChannelBuffers.wrappedBuffer(argString.getBytes(Charsets.UTF_8)));
    }
    httpHandler.startProgram(request, responder, appId, type, flowId);
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, "start " + type + " failed");
  }

  public static void stopProgram(AppFabricHttpHandler httpHandler, String appId, String flowId,
                                 String type) {

    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/%s/%s/stop", appId, type, flowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    httpHandler.stopProgram(request, responder, appId, type, flowId);
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, "stop " + type + " failed");
  }

  public static String getStatus(AppFabricHttpHandler httpHandler, String appId, String flowId,
                                 String type) {

    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/%s/%s/status", appId, type, flowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    httpHandler.getStatus(request, responder, appId, type, flowId);
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, "get status " + type + " failed");
    Map<String, String> json = responder.decodeResponseContent(new TypeToken<Map<String, String>>() { });
    return json.get("status");
  }

  public static void setFlowletInstances(AppFabricHttpHandler httpHandler, String applicationId,
                                         String flowId, String flowletName, int instances) {

    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/flows/%s/flowlets/%s/instances/%s",
                               applicationId, flowId, flowletName, instances);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    JsonObject json = new JsonObject();
    json.addProperty("instances", instances);
    request.setContent(ChannelBuffers.wrappedBuffer(json.toString().getBytes()));
    httpHandler.setFlowletInstances(request, responder, applicationId, flowId, flowletName);
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, "set flowlet instances failed");
  }

  public static List<String> getSchedules(AppFabricHttpHandler httpHandler, String appId, String wflowId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/workflows/%s/schedules", appId, wflowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.workflowSchedules(request, responder, appId, wflowId);

    List<String> schedules = responder.decodeResponseContent(new TypeToken<List<String>>() { });
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, " getting workflow schedules failed");
    return schedules;
  }

  public static List<RunRecord> getHistory(AppFabricHttpHandler httpHandler, String appId, String wflowId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/workflows/%s/history", appId, wflowId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.runnableHistory(request, responder, appId, "workflows", wflowId);
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, " getting workflow schedules failed");

    List<Map<String, String>> runList = responder.decodeResponseContent(new TypeToken<List<Map<String, String>>>() { });
    List<RunRecord> runRecords = Lists.newArrayList();
    for (Map<String, String> run : runList) {
      runRecords.add(new RunRecord(run.get("runid"), Long.parseLong(run.get("start")),
                                       Long.parseLong(run.get("end")), run.get("status")));
    }
    return runRecords;
  }

  public static void suspend(AppFabricHttpHandler httpHandler, String appId, String wflowId,
                                                     String schedId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/workflows/%s/schedules/%s/suspend", appId, wflowId, schedId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.workflowScheduleSuspend(request, responder, appId, wflowId, schedId);
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, " getting workflow schedules failed");
  }

  public static void resume(AppFabricHttpHandler httpHandler, String appId, String wflowId,
                             String schedId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/workflows/%s/schedules/%s/resume", appId, wflowId, schedId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.workflowScheduleResume(request, responder, appId, wflowId, schedId);
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, " getting workflow schedules failed");
  }

  public static String scheduleStatus(AppFabricHttpHandler httpHandler, String appId, String wflowId,
                             String schedId) {
    MockResponder responder = new MockResponder();
    String uri = String.format("/v2/apps/%s/workflows/%s/schedules/%s/status", appId, wflowId, schedId);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    httpHandler.getScheuleState(request, responder, appId, wflowId, schedId);
    Preconditions.checkArgument(responder.getStatus().getCode() == 200, " getting workflow schedules failed");
    Map<String, String> json = responder.decodeResponseContent(new TypeToken<Map<String, String>>() { });
    return json.get("status");
  }

  /**
   * Given a class generates a manifest file with main-class as class.
   *
   * @param klass to set as Main-Class in manifest file.
   * @return An instance {@link java.util.jar.Manifest}
   */
  public static Manifest getManifestWithMainClass(Class<?> klass) {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, klass.getName());
    return manifest;
  }

  /**
   * @return Returns an instance of {@link com.continuuity.internal.app.deploy.LocalManager}
   */
  public static Manager<Location, ApplicationWithPrograms> getLocalManager() {
    ManagerFactory<Location, ApplicationWithPrograms> factory =
      getInjector().getInstance(Key.get(new TypeLiteral<ManagerFactory<Location, ApplicationWithPrograms>>() { }));

    return factory.create(new ProgramTerminator() {
      @Override
      public void stop(Id.Account id, Id.Program programId, com.continuuity.app.program.Type type) throws Exception {
        //No-op
      }
    });
  }

  public static void deployApplication(Class<?> application) throws Exception {
    deployApplication(application,
                      "app-" + TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) + ".jar");
  }

  /**
   *
   */
  public static void deployApplication(Class<?> applicationClz, String fileName) throws Exception {
    Location deployedJar =
      deployApplication(getInjector().getInstance(AppFabricHttpHandler.class),
                        getInjector().getInstance(LocationFactory.class),
                        fileName, applicationClz);
    deployedJar.delete(true);
  }

  public static ApplicationWithPrograms deployApplicationWithManager(Class<?> appClass,
                                                                     final Supplier<File> folderSupplier)
    throws Exception {

    Location deployedJar = createAppJar(appClass);
    try {
      ApplicationWithPrograms appWithPrograms = getLocalManager().deploy(DefaultId.ACCOUNT, null, deployedJar).get();
      // Transform program to get loadable, as the one created in deploy pipeline is not loadable.

      List<Program> programs = ImmutableList.copyOf(Iterables.transform(appWithPrograms.getPrograms(),
                                                                        new Function<Program, Program>() {
            @Override
            public Program apply(Program program) {
              try {
                return Programs.createWithUnpack(program.getJarLocation(), folderSupplier.get());
              } catch (IOException e) {
                throw Throwables.propagate(e);
              }
            }
          }
      ));
      return new ApplicationWithPrograms(appWithPrograms.getAppSpecLoc(), programs);
    } finally {
      deployedJar.delete(true);
    }
  }

  public static Location createAppJar(Class<?> appClass) {
    LocalLocationFactory lf = new LocalLocationFactory();
    return lf.create(JarFinder.getJar(appClass, getManifestWithMainClass(appClass)));
  }

  public static Location deployApplication(AppFabricHttpHandler httpHandler,
                                    LocationFactory locationFactory,
                                    final String applicationId,
                                    Class<?> applicationClz,
                                    File...bundleEmbeddedJars) throws Exception {

    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    Location deployedJar = locationFactory.create(createDeploymentJar(applicationClz, bundleEmbeddedJars).toURI());
    LOG.info("Created deployedJar at {}", deployedJar.toURI().toASCIIString());

    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v2/apps");
    request.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", applicationId + ".jar");
    MockResponder mockResponder = new MockResponder();
    BodyConsumer bodyConsumer = httpHandler.deploy(request, mockResponder);

    BufferFileInputStream is = new BufferFileInputStream(deployedJar.getInputStream(), 100 * 1024);
    try {
      byte[] chunk = is.read();
      while (chunk.length > 0) {
        mockResponder = new MockResponder();
        bodyConsumer.chunk(ChannelBuffers.wrappedBuffer(chunk), mockResponder);
        Preconditions.checkState(mockResponder.getStatus() == null, "failed to deploy app");
        chunk = is.read();
      }
      mockResponder = new MockResponder();
      bodyConsumer.finished(mockResponder);
      Preconditions.checkState(mockResponder.getStatus().getCode() == 200, "failed to deploy app");
    } catch (Exception e) {
        throw Throwables.propagate(e);
    } finally {
      is.close();
    }
    return deployedJar;
  }

  private static File createDeploymentJar(Class<?> clz, File...bundleEmbeddedJars) throws IOException {
    ApplicationBundler bundler = new ApplicationBundler(ImmutableList.of("com.continuuity.api",
                                                                         "org.apache.hadoop",
                                                                         "org.apache.hbase",
                                                                         "org.apache.hive"));
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    Location jarLocation = locationFactory.create(clz.getName()).getTempFile(".jar");
    bundler.createBundle(jarLocation, clz);

    Location deployJar = locationFactory.create(clz.getName()).getTempFile(".jar");

    // Creates Manifest
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, clz.getName());

    // Create the program jar for deployment. It removes the "classes/" prefix as that's the convention taken
    // by the ApplicationBundler inside Twill.
    JarOutputStream jarOutput = new JarOutputStream(deployJar.getOutputStream(), manifest);
    try {
      JarInputStream jarInput = new JarInputStream(jarLocation.getInputStream());
      try {
        JarEntry jarEntry = jarInput.getNextJarEntry();
        while (jarEntry != null) {
          boolean isDir = jarEntry.isDirectory();
          String entryName = jarEntry.getName();
          if (!entryName.equals("classes/")) {
            if (entryName.startsWith("classes/")) {
              jarEntry = new JarEntry(entryName.substring("classes/".length()));
            } else {
              jarEntry = new JarEntry(entryName);
            }
            jarOutput.putNextEntry(jarEntry);

            if (!isDir) {
              ByteStreams.copy(jarInput, jarOutput);
            }
          }

          jarEntry = jarInput.getNextJarEntry();
        }
      } finally {
        jarInput.close();
      }

      for (File embeddedJar : bundleEmbeddedJars) {
        JarEntry jarEntry = new JarEntry("lib/" + embeddedJar.getName());
        jarOutput.putNextEntry(jarEntry);
        Files.copy(embeddedJar, jarOutput);
      }

    } finally {
      jarOutput.close();
    }

    return new File(deployJar.toURI());
  }
}

