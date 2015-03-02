/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Represents information of an application that is undergoing deployment.
 */
public final class ApplicationDeployable implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationDeployable.class);

  private final CConfiguration cConf;
  private final Id.Application id;
  private final ApplicationSpecification specification;
  private final ApplicationSpecification existingAppSpec;
  private final ApplicationDeployScope applicationDeployScope;
  private final Location location;
  private File unpackDir;
  private ClassLoader classLoader;

  public ApplicationDeployable(CConfiguration cConf, Id.Application id, ApplicationSpecification specification,
                               @Nullable ApplicationSpecification existingAppSpec,
                               ApplicationDeployScope applicationDeployScope,
                               Location location) {
    this.cConf = cConf;
    this.id = id;
    this.specification = specification;
    this.existingAppSpec = existingAppSpec;
    this.applicationDeployScope = applicationDeployScope;
    this.location = location;
  }

  public Id.Application getId() {
    return id;
  }

  public ApplicationSpecification getSpecification() {
    return specification;
  }

  @Nullable
  public ApplicationSpecification getExistingAppSpec() {
    return existingAppSpec;
  }

  public ApplicationDeployScope getApplicationDeployScope() {
    return applicationDeployScope;
  }

  public Location getLocation() {
    return location;
  }

  public synchronized ClassLoader getClassLoader() {
    if (classLoader != null) {
      return classLoader;
    }

    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    unpackDir = DirUtils.createTempDir(tmpDir);

    try {
      BundleJarUtil.unpackProgramJar(location, unpackDir);

      // Create a ProgramClassLoader with the CDAP system ClassLoader as filter parent
      classLoader = ProgramClassLoader.create(unpackDir, ApplicationDeployable.class.getClassLoader());
      return classLoader;
    } catch (Exception e) {
      try {
        DirUtils.deleteDirectoryContents(unpackDir);
      } catch (IOException ioe) {
        // OK to ignore. Just log a warn.
        LOG.warn("Failed to delete directory {}", unpackDir, ioe);
      }
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (unpackDir != null) {
      DirUtils.deleteDirectoryContents(unpackDir);
    }
  }
}
