/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.api.service;

import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metadata.MetadataWriter;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.security.store.SecureStore;

/**
 * Context for {@link Service}.
 */
public interface ServiceContext extends RuntimeContext, ServiceDiscoverer, MessagingContext,
    DatasetContext, PluginContext, Transactional, SecureStore, MetadataReader, MetadataWriter,
    ArtifactManager {

  /**
   * Returns the specification used to configure {@link Service} bounded to this context.
   */
  ServiceSpecification getSpecification();

  /**
   * @return number of instances of this handler.
   */
  int getInstanceCount();

  /**
   * @return instance id of this handler.
   */
  int getInstanceId();
}
