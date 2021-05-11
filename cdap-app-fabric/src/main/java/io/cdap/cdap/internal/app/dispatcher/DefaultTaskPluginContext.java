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

import com.google.inject.Inject;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.task.TaskPluginContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.DefaultPluginConfigurer;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactFinder;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * DefaultTaskPluginContext
 */
public class DefaultTaskPluginContext implements TaskPluginContext {

  private CConfiguration cConf;
  private PluginFinder pluginFinder;
  private final ArtifactFinder artifactFinder;
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTaskPluginContext.class);

  @Inject
  DefaultTaskPluginContext(CConfiguration cConf, PluginFinder pluginFinder, ArtifactFinder artifactFinder) {
    this.cConf = cConf;
    this.pluginFinder = pluginFinder;
    this.artifactFinder = artifactFinder;
  }

  private PluginConfigurer createPluginConfigurer(String namespace, ArtifactId artifactId, ClassLoader appClassLoader) {
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();


    try {
      LOG.info("Got request creating plugin with " + namespace + ", and " + artifactId);
      File pluginsDir = Files.createTempDirectory(tmpDir.toPath(), "plugins").toFile();
      //Should this have a spark filter ? See SparkProgramRunner
      //FilterClassLoader filterClassLoader = FilterClassLoader.create(Programs.class.getClassLoader());
      /*Location artifactLocation = artifactFinder
        .getArtifactLocation(new ArtifactId(namespace, artifactId.getArtifact(), artifactId.getVersion()));
      File unpackedDir = DirUtils.createTempDir(tmpDir);
      BundleJarUtil.prepareClassLoaderFolder(artifactLocation, unpackedDir);
      LOG.info("Unpacked artifact in " + unpackedDir);
      ProgramClassLoader programClassLoader = new ProgramClassLoader(cConf, unpackedDir,
                                                                     Thread.currentThread().getContextClassLoader());*/
      PluginInstantiator instantiator = new PluginInstantiator(cConf, appClassLoader, pluginsDir);
      /*closeables.add(() -> {
        try {
          instantiator.close();
        } finally {
          DirUtils.deleteDirectoryContents(pluginsDir, true);
        }
      });*/
      LOG.info("instantiator created with programclassloader " + instantiator + ":" + appClassLoader);
      return new DefaultPluginConfigurer(artifactId, new NamespaceId(namespace), instantiator, pluginFinder);
    } catch (IOException e) {
      LOG.error("IOException", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public PluginConfigurer createPluginConfigurer(String namespace, String artifactName, String artifactVersion,
                                                 ClassLoader appClassLoader) {
    return createPluginConfigurer(namespace, new ArtifactId(namespace, artifactName, artifactVersion), appClassLoader);
  }
}
