/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.task.factory;

import com.google.inject.Inject;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramLogsFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordsFetcher;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.support.SupportBundleState;
import io.cdap.cdap.support.task.SupportBundlePipelineInfoTask;

/**
 * Support bundle
 */
public class SupportBundlePipelineInfoTaskFactory implements SupportBundleTaskFactory {

  private final RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher;
  private final RemoteProgramLogsFetcher remoteProgramLogsFetcher;
  private final RemoteApplicationDetailFetcher remoteApplicationDetailFetcher;
  private final RemoteMetricsSystemClient remoteMetricsSystemClient;

  @Inject
  public SupportBundlePipelineInfoTaskFactory(
      RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher,
      RemoteProgramLogsFetcher remoteProgramLogsFetcher,
      RemoteApplicationDetailFetcher remoteApplicationDetailFetcher,
      RemoteMetricsSystemClient remoteMetricsSystemClient) {
    this.remoteProgramRunRecordsFetcher = remoteProgramRunRecordsFetcher;
    this.remoteProgramLogsFetcher = remoteProgramLogsFetcher;
    this.remoteApplicationDetailFetcher = remoteApplicationDetailFetcher;
    this.remoteMetricsSystemClient = remoteMetricsSystemClient;
  }

  @Override
  public SupportBundlePipelineInfoTask create(SupportBundleState supportBundleState) {
    return new SupportBundlePipelineInfoTask(
        supportBundleState.getUuid(),
        supportBundleState.getNamespaceList(),
        supportBundleState.getAppId(),
        supportBundleState.getBasePath(),
        remoteApplicationDetailFetcher,
        remoteProgramRunRecordsFetcher,
        remoteProgramLogsFetcher,
        supportBundleState.getWorkflowName(),
        remoteMetricsSystemClient,
        supportBundleState.getSupportBundleJob(),
        supportBundleState.getMaxRunsPerPipeline());
  }
}
