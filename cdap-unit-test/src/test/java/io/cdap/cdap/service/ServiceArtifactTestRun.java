/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.service;

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.service.Service;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Unit test for the {@link ArtifactManager} from {@link Service}.
 */
public class ServiceArtifactTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static File artifactJar;

  @BeforeClass
  public static void init() throws Exception {
    artifactJar = createArtifactJar(ServiceArtifactApp.class);
    addArtifact(NamespaceId.DEFAULT.artifact("dummybase", "1.0.0"), createSingleClassJar(DummyPluginBase.class));
    addArtifact(NamespaceId.DEFAULT.artifact("dummy", "1.0.0"), createSingleClassJar(DummyPlugin.class));
  }

  private static File createSingleClassJar(Class<?> cls) throws IOException {
    File jarFile = TMP_FOLDER.newFile();
    try (JarOutputStream os = new JarOutputStream(new FileOutputStream(jarFile))) {
      String resourceName = cls.getName().replace('.', '/') + ".class";
      URL url = cls.getClassLoader().getResource(resourceName);
      Assert.assertNotNull(url);
      os.putNextEntry(new JarEntry(resourceName));
      ByteStreams.copy(url.openStream(), os);
    }

    return jarFile;
  }

  @Test
  public void testServiceArtifact() throws Exception {
    ApplicationManager appManager = deployWithArtifact(ServiceArtifactApp.class, artifactJar);
    ServiceManager serviceManager = appManager.getServiceManager("artifact").start();

    URL serviceURL = serviceManager.getServiceURL(30, TimeUnit.SECONDS);
    Assert.assertNotNull(serviceURL);

    URL listURL = serviceURL.toURI().resolve("list").toURL();
    try (Reader reader = new InputStreamReader(listURL.openStream(), StandardCharsets.UTF_8)) {
      List<ArtifactInfo> artifacts = new Gson().fromJson(reader, new TypeToken<List<ArtifactInfo>>() { }.getType());

      // It should have the test app, and two plugin artifacts
      Assert.assertEquals(3, artifacts.size());

      Assert.assertTrue(artifacts.stream()
                          .anyMatch(info -> info.getName().equals(ServiceArtifactApp.class.getSimpleName())));
      Assert.assertTrue(artifacts.stream().anyMatch(info -> info.getName().equals("dummybase")));
      Assert.assertTrue(artifacts.stream().anyMatch(info -> info.getName().equals("dummy")));
    }

    URL loadURL = serviceURL.toURI()
      .resolve("load?parent=dummybase&plugin=dummy&class=" + DummyPlugin.class.getName()).toURL();
    HttpURLConnection urlConn = (HttpURLConnection) loadURL.openConnection();
    Assert.assertEquals(200, urlConn.getResponseCode());
    try (Reader reader = new InputStreamReader(urlConn.getInputStream(), StandardCharsets.UTF_8)) {
      Assert.assertEquals(DummyPlugin.class.getName(), CharStreams.toString(reader));
    }

    serviceManager.stop();
    serviceManager.waitForStopped(10, TimeUnit.SECONDS);
  }
}
