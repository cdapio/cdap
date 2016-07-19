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

package co.cask.cdap.gateway.handlers.meta;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.http.HttpHandler;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

import java.util.Set;

/**
 * Guice module for {@link RemoteSystemOperationsService}
 */
public class RemoteSystemOperationsServiceModule extends PrivateModule {

  @Override
  protected void configure() {
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
      binder(), HttpHandler.class, Names.named(Constants.RemoteSystemOpService.HANDLERS_NAME));

    CommonHandlers.add(handlerBinder);
    handlerBinder.addBinding().to(RemoteLineageWriterHandler.class);
    handlerBinder.addBinding().to(RemotePrivilegeFetcherHandler.class);
    handlerBinder.addBinding().to(RemoteRuntimeStoreHandler.class);
    handlerBinder.addBinding().to(RemoteUsageRegistryHandler.class);
    handlerBinder.addBinding().to(RemoteNamespaceQueryHandler.class);
    expose(Key.get(new TypeLiteral<Set<HttpHandler>>() { },
                   Names.named(Constants.RemoteSystemOpService.HANDLERS_NAME)));
  }
}
