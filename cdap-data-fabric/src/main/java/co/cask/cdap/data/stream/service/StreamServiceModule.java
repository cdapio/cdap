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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.PingHandler;
import co.cask.http.HttpHandler;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;

/**
 * Base module for stream service bindings
 */
public class StreamServiceModule extends PrivateModule {

  @Override
  protected void configure() {
    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                      Names.named(Constants.Stream.STREAM_HANDLER));
    handlerBinder.addBinding().to(StreamHandler.class);
    handlerBinder.addBinding().to(StreamFetchHandler.class);
    handlerBinder.addBinding().to(PingHandler.class);

    bind(StreamMetaStore.class).to(MDSStreamMetaStore.class).in(Scopes.SINGLETON);
    expose(StreamMetaStore.class);
    bind(StreamHttpService.class).in(Scopes.SINGLETON);
    expose(StreamHttpService.class);
  }
}
