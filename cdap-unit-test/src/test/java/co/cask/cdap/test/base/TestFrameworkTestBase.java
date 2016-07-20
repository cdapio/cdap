/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.test.base;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * TestBase for all test framework tests
 */
public class TestFrameworkTestBase extends TestBase {

  @Override
  @After
  public void afterTest() throws Exception {
    try {
      super.afterTest();
    } finally {
      reset();
    }
  }

  /**
   * Creates an artifact jar by tracing dependency from the given {@link Application} class.
   */
  protected static File createArtifactJar(Class<? extends Application> appClass) throws IOException {
    return new File(AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                     appClass).toURI());
  }

  /**
   * Deploys an {@link Application} using the given artifact jar.
   */
  protected static ApplicationManager deployWithArtifact(Class<? extends Application> appClass,
                                                         File artifactJar) throws Exception {
    return deployWithArtifact(appClass, artifactJar, null);
  }

  /**
   * Deploys an {@link Application} using the given artifact jar.
   */
  protected static ApplicationManager deployWithArtifact(NamespaceId namespaceId,
                                                         Class<? extends Application> appClass,
                                                         File artifactJar) throws Exception {
    return deployWithArtifact(namespaceId, appClass, artifactJar, null);
  }

  /**
   * Deploys an {@link Application} using the given artifact jar with an optional config object.
   */
  protected static <T> ApplicationManager deployWithArtifact(Class<? extends Application> appClass,
                                                             File artifactJar, @Nullable T config) throws Exception {
    return deployWithArtifact(NamespaceId.DEFAULT, appClass, artifactJar, config);
  }

  /**
   * Deploys an {@link Application} using the given artifact jar with an optional config object.
   */
  protected static <T> ApplicationManager deployWithArtifact(NamespaceId namespaceId,
                                                             Class<? extends Application> appClass,
                                                             File artifactJar, @Nullable T config) throws Exception {
    ArtifactId artifactId = new ArtifactId(namespaceId.getNamespace(), appClass.getSimpleName(), "1.0-SNAPSHOT");
    addArtifact(artifactId, artifactJar);
    AppRequest<T> appRequest = new AppRequest<>(new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()),
                                                config);
    return deployApplication(namespaceId.app(appClass.getSimpleName()).toId(), appRequest);
  }

  protected void reset() {
    // Retry clear() multiple times. There is a race between removal of RuntimeInfo
    // in the AbstractProgramRuntimeService class and the clear() method, which loops all RuntimeInfo.
    // The reason for the race is because removal is done through callback.
    try {
      int failureCount = 0;
      Exception exception = null;
      while (failureCount < 10) {
        try {
          exception = null;
          clear();
          break;
        } catch (Exception e) {
          exception = e;
          failureCount++;
          TimeUnit.MILLISECONDS.sleep(200);
        }
      }
      
      if (exception != null) {
        throw exception;
      }
    } catch (Exception e) {
      // If really fail to do reset, propagate the exception
      throw Throwables.propagate(e);
    }
  }
}
