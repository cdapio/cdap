/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.logging.guice;

import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.logging.read.DistributedLogReader;
import co.cask.cdap.logging.read.FileLogReader;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.security.impersonation.RemoteUGIProvider;
import co.cask.cdap.security.impersonation.UGIProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * A {@link RuntimeModule} for providing guice modules for {@link LogReader}.
 */
public final class LogReaderRuntimeModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(FileLogReader.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(FileLogReader.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(DistributedLogReader.class);
        bind(UGIProvider.class).to(RemoteUGIProvider.class).in(Scopes.SINGLETON);
      }
    };
  }
}
