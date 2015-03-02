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

package co.cask.cdap.api.service;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.worker.WorkerContext;

/**
 * Context for custom Service workers.
 *
 * @deprecated As of version 2.8.0, replaced by {@link WorkerContext}
 */
@Deprecated
public interface ServiceWorkerContext extends RuntimeContext, ServiceDiscoverer {

  /**
   * Returns the specification used to configure the {@link ServiceWorker} bounded to this context.
   */
  ServiceWorkerSpecification getSpecification();

  /**
   * Execute a set of operations on datasets via a {@link TxRunnable} that are committed as a single transaction.
   * @param runnable The runnable to be executed in the transaction
   */
  void execute(TxRunnable runnable);

  /**
   * Execute a set of operations on datasets via a {@link TxRunnable} that are committed as a single transaction.
   * @param runnable The runnable to be executed in the transaction
   */
  void execute(co.cask.cdap.api.service.TxRunnable runnable);

  /**
   * @return Number of instances of this worker.
   */
  int getInstanceCount();

  /**
   * @return The instance id of this worker.
   */
  int getInstanceId();
}
