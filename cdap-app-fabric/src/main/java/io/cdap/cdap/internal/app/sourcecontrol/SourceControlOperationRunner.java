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

package io.cdap.cdap.internal.app.sourcecontrol;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * This interface is used for defining the execution of SourceControlOperator push/pull/list
 * either for distributed mode or within a thread in a single node.
 * <p/>
 * <p>
 * This interface extends from {@link Callable} with the intent that the callee is responsible for making
 * sure that this runs in a thread that is allowed to timeout the execution of configure.
 * </p>
 */
public interface SourceControlOperationRunner {
  /**
   * Invokes the SourceControlOperator to push applications to linked repository.
   *
   * @return instance of future that callee can time out
   */
  ListenableFuture<PushApplicationsResponse> push(Collection<AppDetailToPush> appDetails);

  /**
   * Invokes the SourceControlOperator to pull specified applications from linked repository.
   *
   * @return instance of future that callee can time out
   */
  ListenableFuture<PullApplicationResponse> pull(String appPathInRepository);

  /**
   * Invokes the SourceControlOperator to scan and return a list of applications from the linked repository.
   *
   * @return instance of future that callee can time out
   */
  ListenableFuture<ListApplicationsResponse> list();
}
