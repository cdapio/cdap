/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.k8s.runtime;

import java.util.concurrent.TimeUnit;

/**
 * A factory for creating instance of {@link KubeTwillController}.
 */
public interface KubeTwillControllerFactory {

  /**
   * Creates a {@link KubeTwillController} instance. The deployment status should report available within the
   * given amount of time.
   *
   * @param timeout timeout for the deployment status to become available
   * @param timeoutUnit unit for the timeout value
   * @return a {@link KubeTwillController}
   */
  KubeTwillController create(long timeout, TimeUnit timeoutUnit);
}
