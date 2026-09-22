/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 * Guice bindings for the task worker manager netty proxy.
 *
 * <p>{@link TaskWorkerManagerService} must be a singleton: it owns the {@link PodLeaseManager} that
 * tracks which task worker pod is leased to which namespace. A second instance would keep a second,
 * independent lease registry and hand the same pod to two namespaces at once, which is exactly the
 * isolation guarantee the proxy exists to provide.
 */
public class TaskWorkerManagerServiceModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(TaskWorkerManagerService.class).in(Scopes.SINGLETON);
    expose(TaskWorkerManagerService.class);
  }
}
