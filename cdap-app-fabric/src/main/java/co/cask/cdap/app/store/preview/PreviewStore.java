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
package co.cask.cdap.app.store.preview;

import co.cask.cdap.api.preview.DataTracer;
import co.cask.cdap.proto.id.ApplicationId;
import com.google.gson.JsonElement;

import java.util.List;
import java.util.Map;

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
}
