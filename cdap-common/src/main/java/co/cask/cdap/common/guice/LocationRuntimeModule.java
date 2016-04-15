/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.namespace.DefaultNamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.runtime.RuntimeModule;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.PrivilegedAction;

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
      bind(NamespacedLocationFactory.class).to(DefaultNamespacedLocationFactory.class);
    }

    @Provides
    @Singleton
    private LocalLocationFactory providesLocalLocationFactory(CConfiguration cConf) {
      return new LocalLocationFactory(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR)));
    }

    @Provides
    @Singleton
    private RootLocationFactory providesRootLocationFactory(CConfiguration cConf) {
      return new RootLocationFactory(new LocalLocationFactory());
    }
  }

  private static final class HDFSLocationModule extends PrivateModule {

    @Override
    protected void configure() {
      bind(NamespacedLocationFactory.class).to(DefaultNamespacedLocationFactory.class);

      expose(LocationFactory.class);
      expose(RootLocationFactory.class);
      expose(NamespacedLocationFactory.class);
    }

    @Provides
    @Singleton
    private RootLocationFactory provideRoot(Configuration hConf) {
      return new RootLocationFactory(new FileContextLocationFactory(hConf));
    }

    @Provides
    @Singleton
    private LocationFactory providesLocationFactory(final Configuration hConf, CConfiguration cConf) {
      final String namespace = cConf.get(Constants.CFG_HDFS_NAMESPACE);
      LOG.info("HDFS namespace is " + namespace);

      String hdfsUser = cConf.get(Constants.CFG_HDFS_USER);
      UserGroupInformation ugi;
      try {
        if (hdfsUser == null || UserGroupInformation.isSecurityEnabled()) {
          if (hdfsUser != null) {
            LOG.debug("Ignoring configuration {}={}, running on secure Hadoop", Constants.CFG_HDFS_USER, hdfsUser);
          }
          LOG.debug("Getting filesystem for current user");
          ugi = UserGroupInformation.getCurrentUser();
        } else {
          LOG.debug("Getting filesystem for user {}", hdfsUser);
          ugi = UserGroupInformation.createRemoteUser(hdfsUser);
        }
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }

      return ugi.doAs(new PrivilegedAction<LocationFactory>() {
        @Override
        public LocationFactory run() {
          return new FileContextLocationFactory(hConf, namespace);
        }
      });
    }
  }
}
