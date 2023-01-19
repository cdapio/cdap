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
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.internal.app.RemoteTaskExecutor;

import java.util.Collection;

/**
 * This class is used for defining the execution of SourceControlOperator push/pull/list
 * in distributed mode.
 */
public class RemoteSourceControlOperationRunner implements SourceControlOperationRunner {

  private final RemoteTaskExecutor remoteTaskExecutor;

  @Inject
  public RemoteSourceControlOperationRunner(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
                                            RemoteClientFactory remoteClientFactory) {
    this.remoteTaskExecutor = new RemoteTaskExecutor(cConf, metricsCollectionService, remoteClientFactory,
                                                     RemoteTaskExecutor.Type.TASK_WORKER);
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
