/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.task.RunnableTask;
import io.cdap.cdap.api.task.RunnableTaskRequest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactFinder;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * RunnableTaskLauncher launches a runnable task.
 */
public class RunnableTaskLauncher {
  private final CConfiguration cConfig;
  private final ArtifactFinder artifactFinder;
  private final File tmpDir;
  private static final Logger LOG = LoggerFactory.getLogger(RunnableTaskLauncher.class);

  public RunnableTaskLauncher(CConfiguration cConfig, @Nullable ArtifactFinder artifactFinder) {
    this.cConfig = cConfig;
    this.artifactFinder = artifactFinder;
    File tmpDir = new File(cConfig.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConfig.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(tmpDir);
  }

  public byte[] launchRunnableTask(RunnableTaskRequest request) throws Exception {

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (request.getServiceArtifactId() != null && artifactFinder != null) {
      io.cdap.cdap.api.artifact.ArtifactId serviceArtifactId = request.getServiceArtifactId();
      Location artifactLocation = artifactFinder.getArtifactLocation(
        new ArtifactId(NamespaceId.SYSTEM.getNamespace(), serviceArtifactId.getName(),
                       serviceArtifactId.getVersion().getVersion()));

      LOG.info("Got location from remote in launchRunnableTask " + artifactLocation.toURI().toString());
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      BundleJarUtil.prepareClassLoaderFolder(artifactLocation, unpackedDir);
      ProgramClassLoader programClassLoader = new ProgramClassLoader(cConfig, unpackedDir,
                                                                     classLoader);
      //DirectoryClassLoader directoryClassLoader =
       // new DirectoryClassLoader(unpackedDir, classLoader, "lib");
      classLoader = new CloseableClassLoader(programClassLoader, () -> {
        try {
          Closeables.closeQuietly(programClassLoader);
          DirUtils.deleteDirectoryContents(unpackedDir);
        } catch (IOException e) {
          LOG.error("Failed to delete directory {}", unpackedDir, e);
        }
      });
      LOG.info("Created classloader " + programClassLoader);
    }

    Class<?> clazz = classLoader.loadClass(request.getClassName());
    Injector injector = Guice.createInjector(new RunnableTaskModule(cConfig));
    Object obj = injector.getInstance(clazz);
    if (!(obj instanceof RunnableTask)) {
      throw new ClassCastException(String.format("%s is not a RunnableTask", request.getClassName()));
    }
    RunnableTask runnableTask = (RunnableTask) obj;
    runnableTask.setParentClassLoader(classLoader);
    if (runnableTask.start().get() != Service.State.RUNNING) {
      throw new Exception(String.format("service %s failed to start", request.getClassName()));
    }
    byte[] bytes = runnableTask.runTask(request.getParam());
    LOG.info("Task returned response " + new String(bytes));
    return bytes;
  }
}
