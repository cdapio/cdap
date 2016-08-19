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
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.PreviewId;
import org.apache.twill.api.logging.LogEntry;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Interface used to start preview and also retrieve the information associated with a preview.
 */
public interface PreviewManager {

  /**
   * Start the preview of an application config provided as an input.
   * @param namespaceId the id of 
   * @param config the config for the preview
   * @return the unique {@link PreviewId} generated for the preview run
   * @throws Exception if there were any error during starting
   */
  PreviewId start(NamespaceId namespaceId, String config) throws Exception;

  /**
   * Get the status for the specified {@link PreviewId}.
   * @param previewId the id of the preview for which status is to be returned
   * @return the status associated with the preview
   * @throws NotFoundException if the previewId is not found
   */
  PreviewStatus getStatus(PreviewId previewId) throws NotFoundException;

  /**
   * Stop the preview identified by previewId.
   * @param previewId id of the preview
   * @throws Exception if the previewId is not found or if there were any error during stop
   */
  void stop(PreviewId previewId) throws Exception;

  /**
   * Get the data associated with the preview.
   * @param previewId the id associated with the preview
   * @return the {@link Map} of logger name to properties associated with the logger for a given preview
   * @throws NotFoundException if the previewId is not found
   */
  Map<String, Map<String, List<Object>>> getData(PreviewId previewId) throws NotFoundException;

  /**
   * Get metric associated with the preview.
   * @param previewId the id of the preview
   * @return the {@link Collection} of metrics emitted during the preview run
   * @throws NotFoundException if the previewId is not found
   */
  Collection<MetricTimeSeries> getMetrics(PreviewId previewId) throws NotFoundException;

  /**
   * Get the logs for the preview.
   * @param previewId the id of the preview for which logs to be fetched
   * @return the logs
   * @throws NotFoundException if the previewId is not found
   */
  List<LogEntry> getLogs(PreviewId previewId) throws NotFoundException;
}
