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

package co.cask.cdap.common.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A guice module to provide binding for {@link LocationFactory} that uses the {@link FileContextLocationFactory} as
 * the implementation. The actual file system being used is governed by the Hadoop {@link Configuration}, specifically
 * by the {@code fs.defaultFS} configuration.
 */
public class DFSLocationModule extends PrivateModule {

  private static final Logger LOG = LoggerFactory.getLogger(DFSLocationModule.class);

  @Override
  protected void configure() {
    bind(FileContext.class).toProvider(FileContextProvider.class).in(Scopes.SINGLETON);
    bind(LocationFactory.class).toProvider(LocationFactoryProvider.class).in(Scopes.SINGLETON);
    expose(LocationFactory.class);
  }

  /**
   * A Guice {@link Provider} for {@link LocationFactory}.
   */
  private static final class LocationFactoryProvider implements Provider<LocationFactory> {

    private final CConfiguration cConf;
    private final Configuration hConf;
    private final Provider<FileContext> staticFileContextProvider;

    @Inject
    private LocationFactoryProvider(CConfiguration cConf, Configuration hConf,
                                    Provider<FileContext> staticFileContextProvider) {
      this.cConf = cConf;
      this.hConf = hConf;
      this.staticFileContextProvider = staticFileContextProvider;
    }

    @Override
    public LocationFactory get() {
      String namespace = cConf.get(Constants.CFG_HDFS_NAMESPACE);
      LOG.debug("HDFS namespace is {}", namespace);

      // This FileContextLocationFactory supports multiple users from the same process.
      // It is used when security is enabled, which in turn impersonation could occur.
      if (UserGroupInformation.isSecurityEnabled()) {
        return new FileContextLocationFactory(hConf, namespace);
      }

      // In non hadoop secure mode, use the static file context, which operates as single user.
      return new InsecureFileContextLocationFactory(hConf, namespace, staticFileContextProvider.get());
    }
  }
}
