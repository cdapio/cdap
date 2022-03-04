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
import io.cdap.cdap.logging.gateway.handlers.RemoteLogsFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordFetcher;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramRunRecordsFetcher;
import io.cdap.cdap.metadata.RemoteApplicationDetailFetcher;
import io.cdap.cdap.metrics.process.RemoteMetricsSystemClient;
import io.cdap.cdap.support.SupportBundleTaskConfiguration;
import io.cdap.cdap.support.task.SupportBundlePipelineInfoTask;

/**
 * Support bundle pipeline info task factory to create pipeline info task which collect pipeline info and
 * generate them into files.
 */
public class SupportBundlePipelineInfoTaskFactory implements SupportBundleTaskFactory {

  private final RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher;
  private final RemoteLogsFetcher remoteLogsFetcher;
  private final RemoteApplicationDetailFetcher remoteApplicationDetailFetcher;
  private final RemoteMetricsSystemClient remoteMetricsSystemClient;
  private final RemoteProgramRunRecordFetcher remoteProgramRunRecordFetcher;

  @Inject
  public SupportBundlePipelineInfoTaskFactory(RemoteProgramRunRecordsFetcher remoteProgramRunRecordsFetcher,
                                              RemoteLogsFetcher remoteLogsFetcher,
                                              RemoteApplicationDetailFetcher remoteApplicationDetailFetcher,
                                              RemoteMetricsSystemClient remoteMetricsSystemClient,
                                              RemoteProgramRunRecordFetcher remoteProgramRunRecordFetcher) {
    this.remoteProgramRunRecordsFetcher = remoteProgramRunRecordsFetcher;
    this.remoteLogsFetcher = remoteLogsFetcher;
    this.remoteApplicationDetailFetcher = remoteApplicationDetailFetcher;
    this.remoteMetricsSystemClient = remoteMetricsSystemClient;
    this.remoteProgramRunRecordFetcher = remoteProgramRunRecordFetcher;
  }

  @Override
  public SupportBundlePipelineInfoTask create(SupportBundleTaskConfiguration taskConfiguration) {
    return new SupportBundlePipelineInfoTask(taskConfiguration.getUuid(), taskConfiguration.getNamespaces(),
                                             taskConfiguration.getApp(), taskConfiguration.getRun(),
                                             taskConfiguration.getBasePath(), remoteApplicationDetailFetcher,
                                             remoteProgramRunRecordsFetcher, remoteLogsFetcher,
                                             taskConfiguration.getProgramType(), taskConfiguration.getProgramName(),
                                             remoteMetricsSystemClient, taskConfiguration.getSupportBundleJob(),
                                             taskConfiguration.getMaxRunsPerProgram(), remoteProgramRunRecordFetcher);
  }
}
