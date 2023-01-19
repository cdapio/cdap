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
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;

import java.util.Collection;
import javax.inject.Inject;

/**
 * This class is used for defining the execution of SourceControlOperator push/pull/list
 * within a thread in a single node.
 */
public class InMemorySourceControlOperationRunner implements SourceControlOperationRunner {

  private final CConfiguration cConf;

  @Inject
  public InMemorySourceControlOperationRunner(CConfiguration cConf,
                                              MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
  }

  @Override
  public ListenableFuture<PushApplicationsResponse> push(Collection<AppDetailToPush> appDetails) {
    return null;
  }

  @Override
  public ListenableFuture<PullApplicationResponse> pull(String appPathInRepository) {
    return null;
  }

  @Override
  public ListenableFuture<ListApplicationsResponse> list() {
    return null;
  }
}
