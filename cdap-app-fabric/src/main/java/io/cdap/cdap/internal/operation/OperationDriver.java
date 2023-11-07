/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.cdap.cdap.proto.operation.OperationResource;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.twill.common.Threads;

/**
 * A Service for executing {@link LongRunningOperation}.
 */
class OperationDriver extends AbstractExecutionThreadService {

  private final LongRunningOperation operation;
  private final LongRunningOperationContext context;
  private ExecutorService executor;

  OperationDriver(LongRunningOperation operation, LongRunningOperationContext context) {
    this.operation = operation;
    this.context = context;
  }

  @Override
  protected void run() throws Exception {
    Set<OperationResource> resources = operation.run(context).get();
    context.updateOperationResources(resources);
  }

  @Override
  protected void triggerShutdown() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Override
  protected Executor executor() {
    executor = Executors.newSingleThreadExecutor(
        Threads.createDaemonThreadFactory("operation-runner-%d")
    );
    return executor;
  }
}
