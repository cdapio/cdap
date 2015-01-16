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
package co.cask.cdap.common.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Provides Guice bindings for LocationFactory in different runtime environment.
 */
public final class LocationRuntimeModule extends RuntimeModule {
  private static final Logger LOG = LoggerFactory.getLogger(LocationRuntimeModule.class);

  @Override
  public Module getInMemoryModules() {
    return new LocalLocationModule();
  }

  @Override
  public Module getStandaloneModules() {
    return new LocalLocationModule();
  }

  @Override
  public Module getDistributedModules() {
    return new HDFSLocationModule();
  }

  private static final class LocalLocationModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(LocationFactory.class).to(LocalLocationFactory.class);
    }

    @Provides
    @Singleton
    private LocalLocationFactory providesLocalLocationFactory(CConfiguration cConf) {
      return new LocalLocationFactory(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR)));
    }
  }

  private static final class HDFSLocationModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(LocationFactory.class).to(HDFSLocationFactory.class);
    }

    @Provides
    @Singleton
    private HDFSLocationFactory providesHDFSLocationFactory(CConfiguration cConf, Configuration hConf) {
      String hdfsUser = cConf.get(Constants.CFG_HDFS_USER);
      String namespace = cConf.get(Constants.CFG_HDFS_NAMESPACE);
      LOG.info("HDFS namespace is " + namespace);
      FileSystem fileSystem;

      try {
        if (hdfsUser == null || UserGroupInformation.isSecurityEnabled()) {
          if (hdfsUser != null) {
            LOG.debug("Ignoring configuration {}={}, running on secure Hadoop", Constants.CFG_HDFS_USER, hdfsUser);
          }
          LOG.debug("Getting filesystem for current user");
          fileSystem = FileSystem.get(FileSystem.getDefaultUri(hConf), hConf);
        } else {
          LOG.debug("Getting filesystem for user {}", hdfsUser);
          fileSystem = FileSystem.get(FileSystem.getDefaultUri(hConf), hConf, hdfsUser);
        }
        return new HDFSLocationFactory(fileSystem, namespace);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
