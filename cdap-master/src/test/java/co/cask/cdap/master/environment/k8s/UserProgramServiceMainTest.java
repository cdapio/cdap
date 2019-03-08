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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.test.PluginJarHelper;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.internal.app.runtime.k8s.ServiceOptions;
import co.cask.cdap.internal.app.runtime.k8s.UserServiceProgramMain;
import co.cask.cdap.master.environment.ServiceWithPluginApp;
import co.cask.cdap.master.environment.plugin.ConstantCallable;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.Discoverable;
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
import java.net.InetSocketAddress;
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
    // start app fabric in order to deploy an app
    AppFabricServiceMain main = getServiceMainInstance(AppFabricServiceMain.class);

    // Discovery the app-fabric endpoint
    DiscoveryServiceClient discoveryClient = main.getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable appFabricEndpoint = new RandomEndpointStrategy(
      () -> discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP)).pick(20, TimeUnit.SECONDS);

    Assert.assertNotNull(appFabricEndpoint);

    InetSocketAddress addr = appFabricEndpoint.getSocketAddress();
    URI baseURI = URI.create(String.format("http://%s:%d/v3/namespaces/default/",
                                           addr.getHostName(), addr.getPort()));

    // Deploy ServiceWithPluginApp
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, ServiceWithPluginApp.class);

    String appArtifactName = ServiceWithPluginApp.class.getSimpleName();
    String artifactVersion = "1.0.0-SNAPSHOT";

    // deploy app artifact
    HttpRequestConfig requestConfig = new HttpRequestConfig(0, 0);
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
      Discoverable serviceProgramEndpoint = new RandomEndpointStrategy(
        () -> discoveryClient.discover(ServiceDiscoverable.getName(programRunId.getParent())))
        .pick(20, TimeUnit.SECONDS);
      addr = serviceProgramEndpoint.getSocketAddress();
      url = URI.create(String.format("http://%s:%d/v3/namespaces/%s/apps/%s/services/%s/methods/call",
                                     addr.getHostName(), addr.getPort(), programRunId.getNamespace(),
                                     programRunId.getApplication(), programRunId.getProgram())).toURL();
      response = HttpRequests.execute(HttpRequest.get(url).build());
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals(expectedVal, response.getResponseBodyAsString());
    } finally {
      userServiceProgramMain.stop();
      executorService.shutdownNow();
    }
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
