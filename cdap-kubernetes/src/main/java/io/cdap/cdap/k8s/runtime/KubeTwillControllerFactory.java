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

package io.cdap.cdap.k8s.runtime;

import io.kubernetes.client.models.V1ObjectMeta;

import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;

/**
 * A factory for creating instance of {@link KubeTwillController}.
 */
public interface KubeTwillControllerFactory {

  /**
   * Creates a {@link KubeTwillController} instance. The deployment status should report available within the
   * given amount of time.
   *
   * @param resourceType the type of resource
   * @param meta the metadata of the resource object
   * @param timeout timeout for the resource to become available
   * @param timeoutUnit unit for the timeout value
   * @return a {@link KubeTwillController}
   */
  KubeTwillController create(Type resourceType, V1ObjectMeta meta, long timeout, TimeUnit timeoutUnit);
}
