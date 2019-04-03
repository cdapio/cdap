/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.api.service.http;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.metadata.MetadataReader;
import co.cask.cdap.api.metadata.MetadataWriter;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.security.store.SecureStore;

/**
 * The context for a {@link HttpServiceHandler}. Currently contains methods to receive the
 * {@link HttpServiceHandlerSpecification} and the runtime arguments passed by the user.
 */
public interface HttpServiceContext extends RuntimeContext, DatasetContext, ServiceDiscoverer, MessagingContext,
  PluginContext, SecureStore, Transactional, ArtifactManager, MetadataReader, MetadataWriter {

  /**
   * @return the specification bound to this HttpServiceContext
   */
  HttpServiceHandlerSpecification getSpecification();

  /**
   * @return number of instances of this handler.
   */
  int getInstanceCount();

  /**
   * @return instance id of this handler.
   */
  int getInstanceId();

  /**
   * Create a {@link PluginConfigurer} that can be used to instantiate plugins at runtime that were not registered
   * at configure time. Plugins registered by the dynamic configurer live in their own scope and will not
   * conflict with any plugins that were registered by the service when it was configured. Plugins registered by
   * the returned configurer will also not be available through {@link #newPluginInstance(String)} or
   * {@link #newPluginInstance(String, MacroEvaluator)}. Plugins with system scope and plugins in the same namespace
   * as the running service will be visible.
   *
   * The dynamic configurer is meant to be used to create plugins for the lifetime of a single service call and
   * then to be forgotten.
   *
   * @return an dynamic plugin configurer that must be closed
   */
  PluginConfigurer createPluginConfigurer();

  /**
   * Create a {@link PluginConfigurer} that can be used to instantiate plugins at runtime that were not registered
   * at configure time. Plugins registered by the dynamic configurer live in their own scope and will not
   * conflict with any plugins that were registered by the service when it was configured. Plugins registered by
   * the returned configurer will also not be available through {@link #newPluginInstance(String)} or
   * {@link #newPluginInstance(String, MacroEvaluator)}. Plugins with system scope and plugins in the specified
   * namespace will be visible.
   *
   * The dynamic configurer is meant to be used to create plugins for the lifetime of a single service call and
   * then to be forgotten.
   *
   * @param namespace the namespace for user scoped plugins
   * @return an dynamic plugin configurer that must be closed
   */
  PluginConfigurer createPluginConfigurer(String namespace);
}
