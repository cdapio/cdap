/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.api.artifact.ApplicationClass;
import co.cask.cdap.api.artifact.ArtifactClasses;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.gateway.handlers.ArtifactHttpHandler;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.plugin.Plugin1;
import co.cask.cdap.internal.app.runtime.artifact.plugin.Plugin2;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactInfo;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.PluginInfo;
import co.cask.cdap.proto.artifact.PluginSummary;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.jar.Manifest;
import javax.ws.rs.HttpMethod;

/**
 * Tests for {@link ArtifactHttpHandler}
 */
public class ArtifactHttpHandlerTest extends AppFabricTestBase {
  private static final ReflectionSchemaGenerator schemaGenerator = new ReflectionSchemaGenerator();
  private static final Type ARTIFACTS_TYPE = new TypeToken<Set<ArtifactSummary>>() { }.getType();
  private static final Type PLUGIN_SUMMARIES_TYPE = new TypeToken<Set<PluginSummary>>() { }.getType();
  private static final Type PLUGIN_INFOS_TYPE = new TypeToken<Set<PluginInfo>>() { }.getType();
  private static final Type PLUGIN_TYPES_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static ArtifactRepository artifactRepository;
  private static LocationFactory locationFactory;

  @BeforeClass
  public static void setup() throws IOException {
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    locationFactory = new LocalLocationFactory(tmpFolder.newFolder());
  }

  @After
  public void wipeData() throws IOException {
    artifactRepository.clear(Constants.DEFAULT_NAMESPACE_ID);
    artifactRepository.clear(Constants.SYSTEM_NAMESPACE_ID);
  }

  // test deploying an application artifact that has a non-application as its main class
  @Test
  public void testAddBadApp() throws IOException, URISyntaxException {
    Id.Artifact artifactId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "wordcount", "1.0.0");
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(), addAppArtifact(artifactId, ArtifactSummary.class));
  }

  @Test
  public void testNotFound() throws IOException, URISyntaxException {
    Assert.assertTrue(getArtifacts(Constants.DEFAULT_NAMESPACE_ID).isEmpty());
    Assert.assertNull(getArtifacts(Constants.DEFAULT_NAMESPACE_ID, "wordcount"));
    Assert.assertNull(getArtifact(Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "wordcount", "1.0.0")));
  }

  @Test
  public void testAddAndGet() throws IOException, URISyntaxException, UnsupportedTypeException {
    // add 2 versions of the same app that doesn't use config
    Id.Artifact wordcountId1 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "wordcount", "1.0.0");
    Id.Artifact wordcountId2 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "wordcount", "2.0.0");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), addAppArtifact(wordcountId1, WordCountApp.class));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), addAppArtifact(wordcountId2, WordCountApp.class));
    // and 1 version of another app that uses a config
    Id.Artifact configTestAppId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "cfgtest", "1.0.0");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), addAppArtifact(configTestAppId, ConfigTestApp.class));

    // test get /artifacts endpoint
    Set<ArtifactSummary> expectedArtifacts = Sets.newHashSet(
      new ArtifactSummary("wordcount", "1.0.0", false),
      new ArtifactSummary("wordcount", "2.0.0", false),
      new ArtifactSummary("cfgtest", "1.0.0", false)
    );
    Set<ArtifactSummary> actualArtifacts = getArtifacts(Constants.DEFAULT_NAMESPACE_ID);
    Assert.assertEquals(expectedArtifacts, actualArtifacts);

    // test get /artifacts/wordcount endpoint
    expectedArtifacts = Sets.newHashSet(
      new ArtifactSummary("wordcount", "1.0.0", false),
      new ArtifactSummary("wordcount", "2.0.0", false)
    );
    actualArtifacts = getArtifacts(Constants.DEFAULT_NAMESPACE_ID, "wordcount");
    Assert.assertEquals(expectedArtifacts, actualArtifacts);

    // test get /artifacts/cfgtest/versions/1.0.0 endpoint
    Schema appConfigSchema = schemaGenerator.generate(ConfigTestApp.ConfigClass.class);
    ArtifactClasses classes = ArtifactClasses.builder()
      .addApp(new ApplicationClass(ConfigTestApp.class.getName(), "", appConfigSchema))
      .build();
    ArtifactInfo expectedInfo = new ArtifactInfo("cfgtest", "1.0.0", false, classes);
    ArtifactInfo actualInfo = getArtifact(configTestAppId);
    Assert.assertEquals(expectedInfo, actualInfo);
  }

  @Test
  public void testSystemArtifacts() throws Exception {
    // add the app in the default namespace
    Id.Artifact defaultId = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "wordcount", "1.0.0");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), addAppArtifact(defaultId, WordCountApp.class));
    // add a system artifact. currently can't do this through the rest api (by design)
    // so bypass it and use the repository directly
    Id.Artifact systemId = Id.Artifact.from(Constants.SYSTEM_NAMESPACE_ID, "wordcount", "1.0.0");
    File systemArtifact = buildAppArtifact(WordCountApp.class, "wordcount-1.0.0.jar");
    artifactRepository.addArtifact(systemId, systemArtifact, Sets.<ArtifactRange>newHashSet());

    // test get /artifacts?includeSystem=true
    Set<ArtifactSummary> expectedArtifacts = Sets.newHashSet(
      new ArtifactSummary("wordcount", "1.0.0", false),
      new ArtifactSummary("wordcount", "1.0.0", true)
    );
    Set<ArtifactSummary> actualArtifacts = getArtifacts(Constants.DEFAULT_NAMESPACE_ID, true);
    Assert.assertEquals(expectedArtifacts, actualArtifacts);

    // test get /artifacts?includeSystem=false
    expectedArtifacts = Sets.newHashSet(
      new ArtifactSummary("wordcount", "1.0.0", false)
    );
    actualArtifacts = getArtifacts(Constants.DEFAULT_NAMESPACE_ID, false);
    Assert.assertEquals(expectedArtifacts, actualArtifacts);

    // test get /artifacts/wordcount?isSystem=false
    expectedArtifacts = Sets.newHashSet(
      new ArtifactSummary("wordcount", "1.0.0", false)
    );
    actualArtifacts = getArtifacts(Constants.DEFAULT_NAMESPACE_ID, "wordcount", false);
    Assert.assertEquals(expectedArtifacts, actualArtifacts);

    // test get /artifacts/wordcount?isSystem=true
    expectedArtifacts = Sets.newHashSet(
      new ArtifactSummary("wordcount", "1.0.0", true)
    );
    actualArtifacts = getArtifacts(Constants.DEFAULT_NAMESPACE_ID, "wordcount", true);
    Assert.assertEquals(expectedArtifacts, actualArtifacts);

    // test get /artifacts/wordcount/versions/1.0.0?isSystem=false
    ArtifactClasses classes = ArtifactClasses.builder()
      .addApp(new ApplicationClass(WordCountApp.class.getName(), "", null))
      .build();
    ArtifactInfo expectedInfo = new ArtifactInfo("wordcount", "1.0.0", false, classes);
    ArtifactInfo actualInfo = getArtifact(defaultId, false);
    Assert.assertEquals(expectedInfo, actualInfo);

    // test get /artifacts/wordcount/versions/1.0.0?isSystem=true
    expectedInfo = new ArtifactInfo("wordcount", "1.0.0", true, classes);
    actualInfo = getArtifact(defaultId, true);
    Assert.assertEquals(expectedInfo, actualInfo);
  }

  @Test
  public void testGetPlugins() throws IOException, URISyntaxException {
    // add an app for plugins to extend
    Id.Artifact wordCount1Id = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "wordcount", "1.0.0");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), addAppArtifact(wordCount1Id, WordCountApp.class));
    Id.Artifact wordCount2Id = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "wordcount", "2.0.0");
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), addAppArtifact(wordCount2Id, WordCountApp.class));

    // add some plugins.
    // plugins-1.0.0 extends wordcount[1.0.0,2.0.0)
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Plugin1.class.getPackage().getName());
    Id.Artifact pluginsId1 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "plugins", "1.0.0");
    Set<ArtifactRange> plugins1Parents = Sets.newHashSet(new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "wordcount", new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")));
    addPluginArtifact(pluginsId1, Plugin1.class, manifest, plugins1Parents);

    // plugin-2.0.0 extends wordcount[1.0.0,3.0.0)
    Id.Artifact pluginsId2 = Id.Artifact.from(Constants.DEFAULT_NAMESPACE_ID, "plugins", "2.0.0");
    Set<ArtifactRange> plugins2Parents = Sets.newHashSet(new ArtifactRange(
      Constants.DEFAULT_NAMESPACE_ID, "wordcount", new ArtifactVersion("1.0.0"), new ArtifactVersion("3.0.0")));
    addPluginArtifact(pluginsId2, Plugin1.class, manifest, plugins2Parents);

    ArtifactSummary plugins1Artifact = new ArtifactSummary("plugins", "1.0.0", false);
    ArtifactSummary plugins2Artifact = new ArtifactSummary("plugins", "2.0.0", false);

    // get plugin types, should be the same for both
    Set<String> expectedTypes = Sets.newHashSet("dummy", "callable");
    Set<String> actualTypes = getPluginTypes(wordCount1Id);
    Assert.assertEquals(expectedTypes, actualTypes);
    actualTypes = getPluginTypes(wordCount2Id);
    Assert.assertEquals(expectedTypes, actualTypes);

    // get plugin summaries. wordcount1 should see plugins from both plugin artifacts
    Set<PluginSummary> expectedSummaries = Sets.newHashSet(
      new PluginSummary("Plugin1", "dummy", "This is plugin1", Plugin1.class.getName(), plugins1Artifact),
      new PluginSummary("Plugin1", "dummy", "This is plugin1", Plugin1.class.getName(), plugins2Artifact)
    );
    Set<PluginSummary> actualSummaries = getPluginSummaries(wordCount1Id, "dummy");
    Assert.assertEquals(expectedSummaries, actualSummaries);

    expectedSummaries = Sets.newHashSet(
      new PluginSummary(
        "Plugin2", "callable", "Just returns the configured integer", Plugin2.class.getName(), plugins1Artifact),
      new PluginSummary(
        "Plugin2", "callable", "Just returns the configured integer", Plugin2.class.getName(), plugins2Artifact)
    );
    actualSummaries = getPluginSummaries(wordCount1Id, "callable");
    Assert.assertEquals(expectedSummaries, actualSummaries);

    // wordcount2 should only see plugins from plugins2 artifact
    expectedSummaries = Sets.newHashSet(
      new PluginSummary("Plugin1", "dummy", "This is plugin1", Plugin1.class.getName(), plugins2Artifact)
    );
    actualSummaries = getPluginSummaries(wordCount2Id, "dummy");
    Assert.assertEquals(expectedSummaries, actualSummaries);

    expectedSummaries = Sets.newHashSet(
      new PluginSummary(
        "Plugin2", "callable", "Just returns the configured integer", Plugin2.class.getName(), plugins2Artifact)
    );
    actualSummaries = getPluginSummaries(wordCount2Id, "callable");
    Assert.assertEquals(expectedSummaries, actualSummaries);

    // get plugin info. Again, wordcount1 should see plugins from both artifacts
    Map<String, PluginPropertyField> p1Properties = ImmutableMap.of(
      "x", new PluginPropertyField("x", "", "int", true),
      "stuff", new PluginPropertyField("stuff", "", "string", true)
    );
    Map<String, PluginPropertyField> p2Properties = ImmutableMap.of(
      "v", new PluginPropertyField("v", "value to return when called", "int", true)
    );

    Set<PluginInfo> expectedInfos = Sets.newHashSet(
      new PluginInfo("Plugin1", "dummy", "This is plugin1", Plugin1.class.getName(), plugins1Artifact, p1Properties),
      new PluginInfo("Plugin1", "dummy", "This is plugin1", Plugin1.class.getName(), plugins2Artifact, p1Properties)
    );
    Assert.assertEquals(expectedInfos, getPluginInfos(wordCount1Id, "dummy", "Plugin1"));

    expectedInfos = Sets.newHashSet(
      new PluginInfo("Plugin2", "callable", "Just returns the configured integer",
                     Plugin2.class.getName(), plugins1Artifact, p2Properties),
      new PluginInfo("Plugin2", "callable", "Just returns the configured integer",
                     Plugin2.class.getName(), plugins2Artifact, p2Properties)
    );
    Assert.assertEquals(expectedInfos, getPluginInfos(wordCount1Id, "callable", "Plugin2"));

    // while wordcount2 should only see plugins from plugins2 artifact
    expectedInfos = Sets.newHashSet(
      new PluginInfo("Plugin1", "dummy", "This is plugin1", Plugin1.class.getName(), plugins2Artifact, p1Properties)
    );
    Assert.assertEquals(expectedInfos, getPluginInfos(wordCount2Id, "dummy", "Plugin1"));

    expectedInfos = Sets.newHashSet(
      new PluginInfo("Plugin2", "callable", "Just returns the configured integer",
        Plugin2.class.getName(), plugins2Artifact, p2Properties)
    );
    Assert.assertEquals(expectedInfos, getPluginInfos(wordCount2Id, "callable", "Plugin2"));
  }

  private File buildAppArtifact(Class cls, String name) throws IOException {
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, cls, new Manifest());
    File destination = new File(tmpFolder.newFolder(), name);
    Files.copy(Locations.newInputSupplier(appJar), destination);
    return destination;
  }

  private int addAppArtifact(Id.Artifact artifactId, Class cls) throws IOException, URISyntaxException {

    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, cls, new Manifest());

    try (InputStream artifactInputStream = appJar.getInputStream()) {
      return addArtifact(artifactId, artifactInputStream, null);
    } finally {
      appJar.delete();
    }
  }

  private int addPluginArtifact(Id.Artifact artifactId, Class cls, Manifest manifest,
                                Set<ArtifactRange> parents) throws IOException, URISyntaxException {

    Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, cls);

    try (InputStream artifactInputStream = appJar.getInputStream()) {
      return addArtifact(artifactId, artifactInputStream, parents);
    } finally {
      appJar.delete();
    }
  }

  // add an artifact and return the response code
  private int addArtifact(Id.Artifact artifactId, InputStream artifactContents,
                          Set<ArtifactRange> parents) throws IOException, URISyntaxException {
    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts/%s",
      Constants.Gateway.API_VERSION_3, artifactId.getNamespace().getId(), artifactId.getName())).toURL();

    HttpURLConnection urlConn = (HttpURLConnection) endpoint.openConnection();
    urlConn.setRequestMethod(HttpMethod.POST);
    urlConn.setDoOutput(true);
    urlConn.setRequestProperty("Artifact-Version", artifactId.getVersion().getVersion());
    if (parents != null && !parents.isEmpty()) {
      urlConn.setRequestProperty("Artifact-Extends", Joiner.on('/').join(parents));
    }
    ByteStreams.copy(artifactContents, urlConn.getOutputStream());

    int responseCode = urlConn.getResponseCode();
    urlConn.disconnect();

    return responseCode;
  }

  private Set<ArtifactSummary> getArtifacts(Id.Namespace namespace) throws URISyntaxException, IOException {
    URL endpoint = getEndPoint(String.format(
      "%s/namespaces/%s/artifacts", Constants.Gateway.API_VERSION_3, namespace.getId())).toURL();
    return getResults(endpoint, ARTIFACTS_TYPE);
  }

  private Set<ArtifactSummary> getArtifacts(Id.Namespace namespace, boolean includeSystem)
    throws URISyntaxException, IOException {

    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts?includeSystem=%s",
      Constants.Gateway.API_VERSION_3, namespace.getId(), includeSystem)).toURL();
    return getResults(endpoint, ARTIFACTS_TYPE);
  }

  private Set<ArtifactSummary> getArtifacts(Id.Namespace namespace, String name)
    throws URISyntaxException, IOException {

    URL endpoint = getEndPoint(String.format(
      "%s/namespaces/%s/artifacts/%s", Constants.Gateway.API_VERSION_3, namespace.getId(), name)).toURL();
    return getResults(endpoint, ARTIFACTS_TYPE);
  }

  private Set<ArtifactSummary> getArtifacts(Id.Namespace namespace, String name, boolean isSystem)
    throws URISyntaxException, IOException {

    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts/%s?isSystem=%s",
      Constants.Gateway.API_VERSION_3, namespace.getId(), name, isSystem)).toURL();
    return getResults(endpoint, ARTIFACTS_TYPE);
  }

  // get /artifacts/{name}/versions/{version}
  private ArtifactInfo getArtifact(Id.Artifact artifactId) throws URISyntaxException, IOException {
    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts/%s/versions/%s",
      Constants.Gateway.API_VERSION_3, artifactId.getNamespace().getId(),
      artifactId.getName(), artifactId.getVersion().getVersion()))
      .toURL();

    return getResults(endpoint, ArtifactInfo.class);
  }

  // get /artifacts/{name}/versions/{version}?isSystem={isSystem}
  private ArtifactInfo getArtifact(Id.Artifact artifactId, boolean isSystem) throws URISyntaxException, IOException {
    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts/%s/versions/%s?isSystem=%s",
      Constants.Gateway.API_VERSION_3, artifactId.getNamespace().getId(),
      artifactId.getName(), artifactId.getVersion().getVersion(), isSystem))
      .toURL();

    return getResults(endpoint, ArtifactInfo.class);
  }

  // get /artifacts/{name}/versions/{version}/extensions
  private Set<String> getPluginTypes(Id.Artifact artifactId) throws URISyntaxException, IOException {
    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts/%s/versions/%s/extensions",
      Constants.Gateway.API_VERSION_3, artifactId.getNamespace().getId(),
      artifactId.getName(), artifactId.getVersion().getVersion()))
      .toURL();

    return getResults(endpoint, PLUGIN_TYPES_TYPE);
  }

  // get /artifacts/{name}/versions/{version}/extensions/{plugin-type}
  private Set<PluginSummary> getPluginSummaries(Id.Artifact artifactId,
                                         String pluginType) throws URISyntaxException, IOException {
    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts/%s/versions/%s/extensions/%s",
      Constants.Gateway.API_VERSION_3, artifactId.getNamespace().getId(),
      artifactId.getName(), artifactId.getVersion().getVersion(), pluginType))
      .toURL();

    return getResults(endpoint, PLUGIN_SUMMARIES_TYPE);
  }

  // get /artifacts/{name}/versions/{version}/extensions/{plugin-type}/plugins/{plugin-name}
  private Set<PluginInfo> getPluginInfos(Id.Artifact artifactId,
                                     String pluginType, String pluginName) throws URISyntaxException, IOException {
    URL endpoint = getEndPoint(String.format("%s/namespaces/%s/artifacts/%s/versions/%s/extensions/%s/plugins/%s",
      Constants.Gateway.API_VERSION_3, artifactId.getNamespace().getId(),
      artifactId.getName(), artifactId.getVersion().getVersion(), pluginType, pluginName))
      .toURL();

    return getResults(endpoint, PLUGIN_INFOS_TYPE);
  }

  // gets the contents from doing a get on the given url as the given type, or null if a 404 is returned
  private <T> T getResults(URL endpoint, Type type) throws IOException {
    HttpURLConnection urlConn = (HttpURLConnection) endpoint.openConnection();
    urlConn.setRequestMethod(HttpMethod.GET);

    int responseCode = urlConn.getResponseCode();
    if (responseCode == HttpResponseStatus.NOT_FOUND.getCode()) {
      return null;
    }
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), responseCode);

    String responseStr = new String(ByteStreams.toByteArray(urlConn.getInputStream()), Charsets.UTF_8);
    urlConn.disconnect();
    return GSON.fromJson(responseStr, type);
  }
}
