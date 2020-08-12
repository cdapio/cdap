/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
package io.cdap.cdap.app.preview;

import com.google.gson.JsonElement;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.metrics.query.MetricsQueryHelper;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Interface used for managing the preview runs.
 */
public interface PreviewManager {

  /**
   * Start the preview of an application config provided as an input in a given namespace.
   * @param namespace the id of the namespace in which preview to be run
   * @param request the {@link AppRequest} with which preview need to be started
   * @return the {@link ApplicationId} assigned to the preview run
   * @throws Exception if there were any error during starting preview
   */
  ApplicationId start(NamespaceId namespace, AppRequest<?> request) throws Exception;

  /**
   * Get the status of the preview.
   * @param applicationId id of the preview application for which preview status is to be returned
   * @return the status of the preview
   * @throws NotFoundException if preview application is not found
   */
  PreviewStatus getStatus(ApplicationId applicationId) throws NotFoundException;

  /**
   * Stop the preview run represented by this {@link ApplicationId}.
   * @param applicationId id of the preview
   * @throws Exception thrown when any error in stopping the preview run
   */
  void stopPreview(ApplicationId applicationId) throws Exception;

  /**
   * Get the data associated with the preview run represented by this {@link PreviewRunner}.
   * @param applicationId the id of the preview application
   * @param tracerName the name of the tracer used for preview
   * @return the {@link Map} of properties associated with the tracer for a given preview
   */
  Map<String, List<JsonElement>> getData(ApplicationId applicationId, String tracerName);

  /**
   * Get the run id of the program executed as a part of preview.
   * @param applicationId the id of the preview application
   * @return the {@link ProgramRunId} associated with the preview or {@code null} if there is no run record
   */
  @Nullable
  ProgramRunId getRunId(ApplicationId applicationId) throws Exception;

  /**
   * Get the helper object to query for metrics for the preview run.
   * @return the {@link MetricsQueryHelper} associated with the preview
   */
  MetricsQueryHelper getMetricsQueryHelper();

  /**
   * Returns a {@link LogReader} for reading logs for the given preview.
   * @return the {@link LogReader} for reading logs for the given preview
   */
  LogReader getLogReader();

  /**
   * Poll the next available request in the queue.
   * @param pollerInfo information about the poller
   * @return {@code PreviewRequest} if such request is available in the queue
   */
  Optional<PreviewRequest> poll(@Nullable byte[] pollerInfo);
}
