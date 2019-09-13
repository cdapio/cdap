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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.service.ServiceSpecification;
import io.cdap.cdap.api.service.http.HttpServiceHandlerSpecification;
import io.cdap.cdap.api.service.http.ServiceHttpEndpoint;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.test.PluginJarHelper;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.k8s.ServiceOptions;
import io.cdap.cdap.internal.app.runtime.k8s.UserServiceProgramMain;
import io.cdap.cdap.master.environment.ServiceWithPluginApp;
import io.cdap.cdap.master.environment.plugin.ConstantCallable;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.client.ClientMessagingService;
import io.cdap.cdap.messaging.data.RawMessage;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

/**
 * Unit test for running an application service.
 */
public class UserProgramServiceMainTest extends MasterServiceMainTestBase {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .create();

  @Test
  public void testProgramService() throws Exception {
    // Deploy ServiceWithPluginApp
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, ServiceWithPluginApp.class);

    String appArtifactName = ServiceWithPluginApp.class.getSimpleName();
    String artifactVersion = "1.0.0-SNAPSHOT";

    URI baseURI = getRouterBaseURI().resolve("v3/namespaces/default/");
    // deploy app artifact
    HttpRequestConfig requestConfig = new HttpRequestConfig(0, 0, false);
    URL url = baseURI.resolve(String.format("artifacts/%s", appArtifactName)).toURL();
    HttpResponse response = HttpRequests.execute(
      HttpRequest
        .post(url)
        .withBody((ContentProvider<? extends InputStream>) appJar::getInputStream)
        .addHeader("Artifact-Version", artifactVersion)
        .build(), requestConfig);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // deploy plugin artifact
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, ConstantCallable.class.getPackage().getName());
    Location pluginJar = PluginJarHelper.createPluginJar(locationFactory, manifest, ConstantCallable.class);

    String pluginArtifactName = "plugin";
    url = baseURI.resolve(String.format("artifacts/%s", pluginArtifactName)).toURL();
    response = HttpRequests.execute(
      HttpRequest
        .post(url)
        .withBody((ContentProvider<? extends InputStream>) pluginJar::getInputStream)
        .addHeader("Artifact-Extends", String.format("%s[1.0.0-SNAPSHOT,10.0.0]", appArtifactName))
        .addHeader("Artifact-Version", artifactVersion)
        .build(), requestConfig);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // deploy app
    String expectedVal = "x";
    ServiceWithPluginApp.Conf appConf =
      new ServiceWithPluginApp.Conf(ConstantCallable.NAME, Collections.singletonMap("val", expectedVal));
    AppRequest<ServiceWithPluginApp.Conf> appRequest =
      new AppRequest<>(new ArtifactSummary(appArtifactName, artifactVersion), appConf);
    url = baseURI.resolve(String.format("apps/%s", ServiceWithPluginApp.NAME)).toURL();
    response = HttpRequests.execute(
      HttpRequest
        .put(url)
        .withBody(GSON.toJson(appRequest))
        .build(), requestConfig);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    ArtifactId appArtifactId = new ArtifactId(appArtifactName, new ArtifactVersion(artifactVersion),
                                              ArtifactScope.USER);
    ArtifactId pluginArtifactId = new ArtifactId(pluginArtifactName, new ArtifactVersion(artifactVersion),
                                                 ArtifactScope.USER);
    ApplicationSpecification appSpec = createSpec(appArtifactId, pluginArtifactId, expectedVal);

    ProgramRunId programRunId = NamespaceId.DEFAULT.app(ServiceWithPluginApp.NAME)
      .service(ServiceWithPluginApp.ServiceWithPlugin.NAME)
      .run(UUID.randomUUID().toString());

    Map<String, String> systemArgs = new HashMap<>();
    systemArgs.put(ProgramOptionConstants.RUN_ID, programRunId.getRun());
    systemArgs.put(ProgramOptionConstants.INSTANCE_ID, "1");
    systemArgs.put(ProgramOptionConstants.INSTANCES, "1");
    systemArgs.put(ProgramOptionConstants.ARTIFACT_ID,
                   Joiner.on(':').join(NamespaceId.DEFAULT.artifact(appArtifactId.getName(),
                                                                    appArtifactId.getVersion().getVersion())
                                         .toIdParts()));
    ProgramOptions programOptions = new SimpleProgramOptions(programRunId.getParent(),
                                                             new BasicArguments(systemArgs),
                                                             new BasicArguments());
    File configFolder = TEMP_FOLDER.newFolder("config");
    File specFile = new File(configFolder, "appspec.json");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(specFile))) {
      GSON.toJson(appSpec, writer);
    }
    File optionsFile = new File(configFolder, "options.json");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(optionsFile))) {
      GSON.toJson(programOptions, writer);
    }
    String bindAddress = InetAddress.getLoopbackAddress().getCanonicalHostName();

    List<String> args = new ArrayList<>(Arrays.asList(initArgs));
    args.add(String.format("--%s=%s", ServiceOptions.APP_SPEC_PATH, specFile.getAbsolutePath()));
    args.add(String.format("--%s=%s", ServiceOptions.PROGRAM_OPTIONS_PATH, optionsFile.getAbsolutePath()));
    args.add(String.format("--%s=%s", ServiceOptions.BIND_ADDRESS, bindAddress));

    // Run the service program. It should fetch the app jar and start the service
    UserServiceProgramMain userServiceProgramMain = new UserServiceProgramMain();
    userServiceProgramMain.init(args.toArray(new String[] { }));
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.execute(userServiceProgramMain::start);

    try {
      URL callUrl = baseURI.resolve(String.format("apps/%s/services/%s/methods/call",
                                                  programRunId.getApplication(), programRunId.getProgram())).toURL();
      Tasks.waitFor(expectedVal, () -> {
        HttpResponse callResponse = HttpRequests.execute(HttpRequest.get(callUrl).build(),
                                                         new DefaultHttpRequestConfig(false));
        return callResponse.getResponseCode() == 200 ? callResponse.getResponseBodyAsString() : null;
      }, 1, TimeUnit.MINUTES);
    } finally {
      userServiceProgramMain.stop();
      executorService.shutdownNow();
    }

    // verify that program status messages were written out
    // Check the status messages instead of checking the program state through the status endpoint
    // because the program is started manually in this test instead of through normal program lifecycle
    // this means the 'starting' state is never written,
    // thus the 'running' state message is ignored and never stored.
    Injector injector = getServiceMainInstance(MessagingServiceMain.class).getInjector();
    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    MessagingService messagingService = new ClientMessagingService(discoveryServiceClient);
    TopicId topicId = NamespaceId.SYSTEM.topic(cConf.get(Constants.AppFabric.PROGRAM_STATUS_EVENT_TOPIC));
    boolean foundRunning = false;
    boolean foundKilled = false;
    try (CloseableIterator<RawMessage> iter = messagingService.prepareFetch(topicId).fetch()) {
      while (iter.hasNext()) {
        RawMessage message = iter.next();
        Notification notification = GSON.fromJson(Bytes.toString(message.getPayload()), Notification.class);
        if (notification.getNotificationType() != Notification.Type.PROGRAM_STATUS) {
          continue;
        }
        ProgramRunId notificationRunID = GSON.fromJson(notification.getProperties().get("programRunId"),
                                                       ProgramRunId.class);
        if (!programRunId.equals(notificationRunID)) {
          continue;
        }
        ProgramRunStatus runStatus = ProgramRunStatus.valueOf(notification.getProperties().get("programStatus"));
        if (runStatus == ProgramRunStatus.RUNNING) {
          foundRunning = true;
        } else if (runStatus == ProgramRunStatus.KILLED) {
          foundKilled = true;
        }
      }
    }
    Assert.assertTrue("Did not find program state messages", foundRunning && foundKilled);
  }

  private ApplicationSpecification createSpec(ArtifactId appArtifactId, ArtifactId pluginArtifactId,
                                              String expectedVal) {
    ServiceHttpEndpoint endpoint = new ServiceHttpEndpoint("GET", "call");
    HttpServiceHandlerSpecification callHandler = new HttpServiceHandlerSpecification(
      ServiceWithPluginApp.HandlerWithPlugin.class.getName(),
      ServiceWithPluginApp.HandlerWithPlugin.class.getSimpleName(),
      "desc",
      Collections.emptyMap(),
      Collections.emptySet(),
      Collections.singletonList(endpoint));
    ServiceSpecification serviceSpecification = new ServiceSpecification(
      ServiceWithPluginApp.ServiceWithPlugin.class.getName(),
      ServiceWithPluginApp.ServiceWithPlugin.NAME, "desc",
      Collections.singletonMap(ServiceWithPluginApp.HandlerWithPlugin.class.getSimpleName(), callHandler),
      new Resources(), 1, Collections.emptyMap());

    PluginPropertyField valField = new PluginPropertyField(ConstantCallable.NAME, "", "string", true, false);
    PluginClass pluginClass =
      new PluginClass(ServiceWithPluginApp.PLUGIN_TYPE, ConstantCallable.NAME, "", ConstantCallable.class.getName(),
                      "conf", Collections.singletonMap("val", valField));
    Plugin plugin = new Plugin(Collections.emptyList(), pluginArtifactId,
                               pluginClass, PluginProperties.builder().add("val", expectedVal).build());
    return new DefaultApplicationSpecification(
      ServiceWithPluginApp.NAME, "desc", "", appArtifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(),
      Collections.singletonMap(ServiceWithPluginApp.ServiceWithPlugin.NAME, serviceSpecification),
      Collections.emptyMap(), Collections.emptyMap(),
      Collections.singletonMap(ServiceWithPluginApp.PLUGIN_ID, plugin));
  }
}
