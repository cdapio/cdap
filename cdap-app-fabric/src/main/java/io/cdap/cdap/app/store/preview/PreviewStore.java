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
package io.cdap.cdap.app.store.preview;

import com.google.gson.JsonElement;
import io.cdap.cdap.api.preview.DataTracer;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.app.preview.PreviewStatus;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramRunId;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface used by {@link DataTracer} to store the preview data.
 */
public interface PreviewStore {

  /**
   * Add the preview data.
   *
   * @param applicationId the id of the program which is logging the preview data
   * @param tracerName the name of the logger used to put the preview data
   * @param propertyName the name of the property for which value is being added
   * @param value the value to be added
   */
  void put(ApplicationId applicationId, String tracerName, String propertyName, Object value);

  /**
   * Get the preview data associated with the given application id.
   *
   * @param applicationId the id of the preview for which preview data to be fetched
   * @param tracerName the name of the tracer used to put the preview data
   * @return the {@link Map} of property and associated values for the program
   */
  Map<String, List<JsonElement>> get(ApplicationId applicationId, String tracerName);

  /**
   * Removes the preview data stored by specified application id
   *
   * @param applicationId the id of the preview for which the data to be removed
   */
  void remove(ApplicationId applicationId);

  /**
   * Save the program run id associated with the preview run
   *
   * @param programRunId the program run id to save
   */
  void setProgramId(ProgramRunId programRunId);

  /**
   * Get the program run id associated with the preview run
   *
   * @param applicationId the preview id
   * @return the program run id of the preview, null if no run has started with the preview id
   */
  @Nullable
  ProgramRunId getProgramRunId(ApplicationId applicationId);

  /**
   * Set the preview status assoicated with the preview run
   *
   * @param applicationId the preview id
   * @param previewStatus the preview status
   */
  void setPreviewStatus(ApplicationId applicationId, PreviewStatus previewStatus);

  /**
   * Get the preview status assoicated with the preview run
   *
   * @param applicationId the preview id
   * @return the preview status of the preview run, null if no run has started with the preview id
   *
   */
  @Nullable
  PreviewStatus getPreviewStatus(ApplicationId applicationId);

  /**
   * Adds the preview request for given application id in the store.
   * {@code getPreviewStatus} call returns status as WAITING once the request
   * is added to the store.
   * @param applicationId the application id corresponding to the request
   * @param appRequest preview request configuration
   */
  void add(ApplicationId applicationId, AppRequest appRequest);

  /**
   * @return list of all waiting requests in waiting state sorted by submit time
   */
  List<PreviewRequest> getAllInWaitingState();

  /**
   * Sets the information about the poller that has acquired the waiting application for running.
   * @param applicationId applicationId the application id of preview
   * @param pollerInfo information about the poller that is going to run the preview
   * @throws ConflictException exception thrown when preview application for which poller info is to be set is not
   *                           in WAITING state
   */
  void setPreviewRequestPollerInfo(ApplicationId applicationId, @Nullable byte[] pollerInfo) throws ConflictException;

  /**
   * @return the poller info associated with the application if it exists, otherwise {@code null} is returned
   */
  @Nullable
  byte[] getPreviewRequestPollerInfo(ApplicationId applicationId);
}
