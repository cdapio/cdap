/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.io.RootLocationFactory;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.FileContextLocationFactory;

/**
 * A Guice Provider for {@link RootLocationFactory}. This {@link RootLocationFactoryProvider} gives the location
 * factory which is the root of the filesystem. To get a location factory based on {link Constants#CFG_HDFS_NAMESPACE}
 * please refer providesLocationFactory in {@link LocationRuntimeModule#getDistributedModules()}
 */
public final class RootLocationFactoryProvider implements Provider<RootLocationFactory> {

  private final FileContext fc;
  private final Configuration hConf;

  @Inject
  public RootLocationFactoryProvider(FileContext fc, Configuration hConf) {
    this.fc = fc;
    this.hConf = hConf;
  }

  @Override
  public RootLocationFactory get() {
    FileContextLocationFactory delegate;
    if (UserGroupInformation.isSecurityEnabled()) {
      delegate = new FileContextLocationFactory(hConf, "/");
    } else {
      delegate = new InsecureFileContextLocationFactory(hConf, "/", fc);
    }
    return new RootLocationFactory(delegate);
  }
}
