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

package co.cask.cdap.data2.audit;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

/**
 * Guice bindings for publishing audit.
 */
public class AuditModule extends RuntimeModule {
  @Override
  public Module getInMemoryModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(AuditPublisher.class).to(InMemoryAuditPublisher.class);
        bind(InMemoryAuditPublisher.class).in(Singleton.class);
        expose(AuditPublisher.class);
        expose(InMemoryAuditPublisher.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(AuditPublisher.class).toProvider(KafkaAuditPublisherProvider.class).in(Scopes.SINGLETON);
        expose(AuditPublisher.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(AuditPublisher.class).toProvider(KafkaAuditPublisherProvider.class).in(Scopes.SINGLETON);
        expose(AuditPublisher.class);
      }
    };
  }

  @SuppressWarnings("unused")
  private static class KafkaAuditPublisherProvider implements Provider<AuditPublisher> {
    private final Injector injector;
    private final CConfiguration cConf;

    @Inject
    public KafkaAuditPublisherProvider(Injector injector, CConfiguration cConf) {
      this.injector = injector;
      this.cConf = cConf;
    }

    @Override
    public AuditPublisher get() {
      if (cConf.getBoolean(Constants.Audit.ENABLED, false)) {
        return injector.getInstance(KafkaAuditPublisher.class);
      }
      return injector.getInstance(NoOpAuditPublisher.class);
    }
  }
}
