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

package io.cdap.cdap.api.service.http;

import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metadata.MetadataWriter;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.security.store.SecureStore;

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
