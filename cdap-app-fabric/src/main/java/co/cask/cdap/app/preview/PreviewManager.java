/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.app.preview;

import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.twill.api.logging.LogEntry;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Interface used for managing the preview lifecycle and retrieval of the preview data.
 */
public interface PreviewManager {

  /**
   * Start the preview of an application config provided as an input.
   * @param namespaceId the id of the namespace
   * @param request the application request containing app configs
   * @return the id of the preview application
   * @throws Exception if there were any error during starting preview
   */
  ApplicationId start(NamespaceId namespaceId, AppRequest<?> request) throws Exception;

  /**
   * Get the status for the specified preview represented by {@link ApplicationId}.
   * @param preview the application id of the preview for which status is to be returned
   * @return the status associated with the preview
   * @throws NotFoundException if the preview application is not found
   */
  PreviewStatus getStatus(ApplicationId preview) throws NotFoundException;

  /**
   * Stop the preview as represented by the {@link ApplicationId}.
   * @param preview the application id of the preview
   * @throws Exception if the preview is not found or if there were any error during stop
   */
  void stop(ApplicationId preview) throws Exception;

  /**
   * Get list of tracers used in the specified preview.
   * @param preview the application id of the preview
   * @return {@link List} of tracers used in the preview
   * @throws NotFoundException if preview application is not found
   */
  List<String> getTracers(ApplicationId preview) throws NotFoundException;

  /**
   * Get the data associated with the preview.
   * @param preview the id associated with the preview
   * @return the {@link Map} of tracer name to properties associated with the tracer for a given preview
   * @throws NotFoundException if the previewId is not found
   */
  Map<String, Map<String, List<Object>>> getData(ApplicationId preview) throws NotFoundException;

  /**
   * Get metric associated with the preview.
   * @param preview the id of the preview
   * @return the {@link Collection} of metrics emitted during the preview run
   * @throws NotFoundException if the previewId is not found
   */
  Collection<MetricTimeSeries> getMetrics(ApplicationId preview) throws NotFoundException;

  /**
   * Get the logs for the preview.
   * @param preview the id of the preview for which logs to be fetched
   * @return the logs
   * @throws NotFoundException if the previewId is not found
   */
  List<LogEntry> getLogs(ApplicationId preview) throws NotFoundException;
}
