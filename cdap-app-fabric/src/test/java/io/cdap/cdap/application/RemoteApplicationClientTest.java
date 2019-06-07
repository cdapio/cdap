/*
 *
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

package io.cdap.cdap.application;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.inject.Injector;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.metadata.AbstractMetadataClient;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.app.plugin.PluginTestApp;
import io.cdap.cdap.internal.app.runtime.artifact.app.plugin.PluginTestRunnable;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.metadata.RemoteMetadataClient;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class RemoteApplicationClientTest extends AppFabricTestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final Id.Artifact APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT, "PluginTest", "1.0.0");

  private static CConfiguration cConf;
  private static File tmpDir;
  private static File systemArtifactsDir1;
  private static File systemArtifactsDir2;
  private static ArtifactRepository artifactRepository;
  private static File appArtifactFile;
  private static Injector injector;

  @BeforeClass
  public static void setup() throws Exception {
    systemArtifactsDir1 = TMP_FOLDER.newFolder();
    systemArtifactsDir2 = TMP_FOLDER.newFolder();
    tmpDir = TMP_FOLDER.newFolder();

    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR,
              systemArtifactsDir1.getAbsolutePath() + ";" + systemArtifactsDir2.getAbsolutePath());
    injector = AppFabricTestHelper.getInjector(cConf);
    artifactRepository = injector.getInstance(ArtifactRepository.class);

    appArtifactFile = createAppJar(PluginTestApp.class, new File(tmpDir, "PluginTest-1.0.0.jar"),
                                   createManifest(ManifestFields.EXPORT_PACKAGE,
                                                  PluginTestRunnable.class.getPackage().getName()));
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  private static File createAppJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                              cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Files.copy(Locations.newInputSupplier(deploymentJar), destFile);
    return destFile;
  }

  private static Manifest createManifest(Object... entries) {
    Preconditions.checkArgument(entries.length % 2 == 0);
    Attributes attributes = new Attributes();
    for (int i = 0; i < entries.length; i += 2) {
      attributes.put(entries[i], entries[i + 1]);
    }
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().putAll(attributes);
    return manifest;
  }

  @Before
  public void setupData() throws Exception {
    artifactRepository.clear(NamespaceId.DEFAULT);
    artifactRepository.addArtifact(APP_ARTIFACT_ID, appArtifactFile, null, null);
  }

  @Test
  public void listApplications() throws Exception {
    AbstractApplicationClient applicationClient = injector.getInstance(RemoteApplicationClient.class);
    applicationClient.listApplications();

  }

  @Test
  public void metadata() throws BadRequestException, IOException, UnauthenticatedException {
    Injector injector = AppFabricTestHelper.getInjector();
    AbstractMetadataClient applicationClient = injector.getInstance(RemoteMetadataClient.class);
    MetadataEntity metadataEntity = MetadataEntity.ofNamespace("default");
    applicationClient.getMetadata(metadataEntity);
  }
}