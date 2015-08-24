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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.AppWithMR;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.Id;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.jar.Manifest;

/**
 */
public class ApplicationLifecycleServiceTest extends AppFabricTestBase {

  private static ApplicationLifecycleService applicationLifecycleService;
  private static Store store;
  private static LocationFactory locationFactory;

  @BeforeClass
  public static void setup() throws Exception {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    store = getInjector().getInstance(DefaultStore.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
  }

  @Test
  public void testUpgrade() throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, "3.1.0");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, AppWithMR.class, manifest);

    Application app = new AppWithMR();
    DefaultAppConfigurer configurer = new DefaultAppConfigurer(app);
    app.configure(configurer, new DefaultApplicationContext());
    ApplicationSpecification appSpec = configurer.createSpecification();

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, appSpec.getName());
    // write an app spec without an artifact id
    store.addApplication(appId, appSpec, appJar);
    appSpec = store.getApplication(appId);
    Assert.assertNull(appSpec.getArtifactId());

    // run upgrade
    applicationLifecycleService.upgrade(false);

    // app sepc should now have an artifact id
    appSpec = store.getApplication(appId);
    Assert.assertEquals(Id.Artifact.from(Id.Namespace.DEFAULT, appSpec.getName(), "3.1.0"), appSpec.getArtifactId());
  }
}
