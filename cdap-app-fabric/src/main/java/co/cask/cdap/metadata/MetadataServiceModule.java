/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.http.HttpHandler;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

import java.util.Set;

/**
 * Guice module for metadata service.
 */
public class MetadataServiceModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new InMemoryMetadataModule();
  }

  @Override
  public Module getStandaloneModules() {
    return new InMemoryMetadataModule();
  }

  @Override
  public Module getDistributedModules() {
    return new DistributedMetadataModule();
  }

  private abstract static class CommonMetadataModule extends PrivateModule {
    @Override
    protected void configure() {
      Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
        binder(), HttpHandler.class, Names.named(Constants.Metadata.METADATA_HANDLERS_NAME));

      CommonHandlers.add(handlerBinder);
      handlerBinder.addBinding().to(MetadataHttpHandler.class);
      handlerBinder.addBinding().to(LineageHandler.class);
      expose(Key.get(new TypeLiteral<Set<HttpHandler>>() { }, Names.named(Constants.Metadata.METADATA_HANDLERS_NAME)));
      
      bind(MetadataAdmin.class).to(DefaultMetadataAdmin.class);
      expose(MetadataAdmin.class);
      bind(MetadataChangePublisher.class).to(getChangePublisher());
      expose(MetadataChangePublisher.class);
    }

    /**
     * @return the {@link MetadataChangePublisher} to bind to.
     */
    abstract Class<? extends MetadataChangePublisher> getChangePublisher();
  }

  private static final class DistributedMetadataModule extends CommonMetadataModule {
    @Override
    Class<? extends MetadataChangePublisher> getChangePublisher() {
      return KafkaMetadataChangePublisher.class;
    }
  }

  private static final class InMemoryMetadataModule extends CommonMetadataModule {
    @Override
    Class<? extends MetadataChangePublisher> getChangePublisher() {
      return NoOpMetadataChangePublisher.class;
    }
  }

  private static final class NoOpMetadataChangePublisher implements MetadataChangePublisher {

    @Override
    public void publish(MetadataChangeRecord change) {
      // no-op
    }
  }
}
