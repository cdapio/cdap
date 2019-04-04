/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.worker;

import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metadata.MetadataWriter;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.security.store.SecureStore;

/**
 * Context for {@link Worker}.
 */
public interface WorkerContext extends RuntimeContext, ServiceDiscoverer, MessagingContext,
  DatasetContext, PluginContext, Transactional, SecureStore, MetadataReader, MetadataWriter {

  /**
   * Returns the specification used to configure {@link Worker} bounded to this context.
   */
  WorkerSpecification getSpecification();

  /**
   * @return number of instances of this worker
   */
  int getInstanceCount();

  /**
   * @return the instance id of this worker
   */
  int getInstanceId();
}
