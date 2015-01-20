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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.common.runtime.RuntimeModule;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * Defines Guice modules in different runtime environments.
 */
public final class StreamServiceRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new StreamServiceModule() {
      @Override
      protected void configure() {
        // For in memory stream, nothing to cleanup
        bind(StreamFileJanitorService.class).to(NoopStreamFileJanitorService.class).in(Scopes.SINGLETON);
        bind(StreamWriterSizeManager.class).to(NoOpStreamWriterSizeManager.class).in(Scopes.SINGLETON);
        super.configure();
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new StreamServiceModule() {
      @Override
      protected void configure() {
        bind(StreamFileJanitorService.class).to(LocalStreamFileJanitorService.class).in(Scopes.SINGLETON);
        bind(StreamWriterSizeManager.class).to(NoOpStreamWriterSizeManager.class).in(Scopes.SINGLETON);
        super.configure();
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new StreamServiceModule() {
      @Override
      protected void configure() {
        bind(StreamFileJanitorService.class).to(DistributedStreamFileJanitorService.class).in(Scopes.SINGLETON);
        bind(StreamWriterSizeManager.class).to(NoOpStreamWriterSizeManager.class).in(Scopes.SINGLETON);
        super.configure();
      }
    };
  }


  /**
   * A {@link StreamFileJanitorService} that does nothing. For in memory stream module only.
   */
  private static final class NoopStreamFileJanitorService extends AbstractService implements StreamFileJanitorService {

    @Override
    protected void doStart() {
      notifyStarted();
    }

    @Override
    protected void doStop() {
      notifyStopped();
    }
  }
}
