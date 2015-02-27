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

package co.cask.cdap.api.worker;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.stream.StreamContext;

/**
 * Context for {@link Worker}.
 */
public interface WorkerContext extends RuntimeContext, ServiceDiscoverer, StreamContext {

  /**
   * Returns the specification used to configure {@link Worker} bounded to this context.
   */
  WorkerSpecification getSpecification();

  /**
   * Execute a set of operations on datasets via a {@link TxRunnable} that are committed as a single transaction.
   * @param runnable the runnable to be executed in the transaction
   */
  void execute(TxRunnable runnable);

  /**
   * @return number of instances of this worker
   */
  int getInstanceCount();

  /**
   * @return the instance id of this worker
   */
  int getInstanceId();
}
