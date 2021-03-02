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

package io.cdap.cdap.security.guice;

import com.google.inject.Module;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.CConfigurationUtil;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.runtime.RuntimeModule;
import org.apache.twill.zookeeper.ZKClient;

/**
 * Security guice modules
 */
//TODO: we need to have separate implementations for inMemoryModule and standaloneModule
public class SecurityModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new InMemorySecurityModule();
  }

  @Override
  public Module getStandaloneModules() {
    return new InMemorySecurityModule();
  }

  /**
   * Deprecated, use the {@link #getDistributedModule(CConfiguration)} instead.
   * @deprecated
   */
  @Override
  public Module getDistributedModules() {
    return new DistributedSecurityModule();
  }

  /**
   * Returns {@code true} if a {@link ZKClient} binding is needed for the distributed module.
   */
  public static SecurityModule getDistributedModule(CConfiguration cConf) {
    // If the file based path is not set explicitly, use ZK.
    // If ZK is set explicitly, always use ZK.
    // This is the backward compatible behavior.
    if (!CConfigurationUtil.isOverridden(cConf, Constants.Security.CFG_FILE_BASED_KEYFILE_PATH)
        || CConfigurationUtil.isOverridden(cConf, Constants.Zookeeper.QUORUM)) {
      return new DistributedSecurityModule();
    }
    return new FileBasedSecurityModule();
  }
}
