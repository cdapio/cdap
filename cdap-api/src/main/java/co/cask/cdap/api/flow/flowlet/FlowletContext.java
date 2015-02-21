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

package co.cask.cdap.api.flow.flowlet;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.stream.StreamContext;

/**
 * This interface represents the Flowlet context.
 */
public interface FlowletContext extends RuntimeContext, DatasetContext, ServiceDiscoverer, StreamContext {
  /**
   * @return Number of instances of this flowlet.
   */
  int getInstanceCount();

  /**
   * @return The instance id of this flowlet.
   */
  int getInstanceId();

  /**
   * @return Name of this flowlet.
   */
  String getName();

  /**
   * @return The specification used to configure this {@link Flowlet} instance.
   */
  FlowletSpecification getSpecification();
}
