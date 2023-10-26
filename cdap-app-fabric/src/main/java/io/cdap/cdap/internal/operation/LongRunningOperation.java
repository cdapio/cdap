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

import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.proto.operation.OperationResource;
import java.util.Set;

/**
 * LongRunningOperation represents a long-running asynchronous operation.
 */
public interface LongRunningOperation {

  /**
   * Run the operation with the given request.
   *
   * @param context the operation context. Contains func to update the resources of the
   *     operation. This would be passed by the runner.
   * @return {@link ListenableFuture} containing the {@link OperationResource}s operation on by the
   *     run
   */
  ListenableFuture<Set<OperationResource>> run(LongRunningOperationContext context)
      throws OperationException;
}
