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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.worker.Worker;

/**
 * Workers for user services must implement this interface.
 *
 * @deprecated As of version 2.8.0, replaced by {@link Worker}
 */
@Deprecated
public interface ServiceWorker extends Runnable, ProgramLifecycle<ServiceWorkerContext> {

  /**
   * Configure a ServiceWorker.
   */
  void configure(ServiceWorkerConfigurer configurer);

  /**
   * Request to stop the running worker.
   * This method will be invoked from a different thread than the one calling the {@link #run()} ) method.
   */
  void stop();
}
