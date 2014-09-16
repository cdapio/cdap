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

package co.cask.cdap.app.deploy;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Callable;

/**
 * This interface is used for defining the execution of configure either for distributed mode
 * or within a thread in a single node.
 * <p/>
 * <p>
 * This interface extends from {@link Callable} with the intent that the callee is responsible for making
 * sure that this runs in a thread that is allowed to timeout the execution of configure.
 * </p>
 */
public interface Configurator {

  /**
   * Invokes the configurator.
   *
   * @return instance of future that callee can timeout
   */
  ListenableFuture<ConfigResponse> config();
}
