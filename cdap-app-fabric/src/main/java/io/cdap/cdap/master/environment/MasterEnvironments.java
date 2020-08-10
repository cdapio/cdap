/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.master.environment;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

/**
 * Utility class for {@link MasterEnvironment} operations.
 */
public final class MasterEnvironments {

  private static final InheritableThreadLocal<MasterEnvironment> MASTER_ENV = new InheritableThreadLocal<>();

  /**
   * Creates a new instance of {@link MasterEnvironment}.
   *
   * @param cConf the CDAP configuration
   * @param envName the master environment name
   * @return a new, initialized instance
   * @throws NotFoundException if the master environment of the given name does not exist
   */
  public static MasterEnvironment create(CConfiguration cConf, String envName) throws NotFoundException {
    MasterEnvironmentExtensionLoader loader = new MasterEnvironmentExtensionLoader(cConf);
    MasterEnvironment masterEnv = loader.get(envName);
    if (masterEnv == null) {
      throw new NotFoundException("Master environment of name " + envName + " does not exist");
    }
    return masterEnv;
  }

  /**
   * Sets the given {@link MasterEnvironment} to a inheritable thread local, or pass in {@code null} to unset it.
   *
   * @return the provided {@link MasterEnvironment}
   */
  public static MasterEnvironment setMasterEnvironment(@Nullable MasterEnvironment masterEnv) {
    if (masterEnv == null) {
      MASTER_ENV.remove();
    } else {
      MASTER_ENV.set(masterEnv);
    }
    return masterEnv;
  }

  /**
   * Gets the {@link MasterEnvironment} that was set earlier by the {@link #setMasterEnvironment(MasterEnvironment)}
   * method.
   *
   * @return the {@link MasterEnvironment} that was set earlier or {@code null} if it was not set
   */
  @Nullable
  public static MasterEnvironment getMasterEnvironment() {
    return MASTER_ENV.get();
  }

  /**
   * Creates a new {@link MasterEnvironmentContext} from the given configurations.
   */
  public static MasterEnvironmentContext createContext(CConfiguration cConf, Configuration hConf,
                                                       String masterEnvName) {
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new DFSLocationModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bindConstant().annotatedWith(Names.named(DefaultMasterEnvironmentContext.MASTER_ENV_NAME)).to(masterEnvName);
          bind(MasterEnvironmentContext.class).to(DefaultMasterEnvironmentContext.class);
        }
      });
    return injector.getInstance(MasterEnvironmentContext.class);
  }

  private MasterEnvironments() {
    // private for util class.
  }
}
