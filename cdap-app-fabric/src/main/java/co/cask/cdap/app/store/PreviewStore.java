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

package co.cask.cdap.app.store;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.preview.PreviewLogger;
import co.cask.cdap.proto.id.PreviewId;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface used by {@link PreviewLogger} to store the preview data.
 */
public interface PreviewStore {

  /**
   * Add the preview data.
   * @param previewId the id of the program which is logging the preview data
   * @param loggerName the name of the logger used to log the preview data
   * @param propertyName the name of the property for which value is being added
   * @param value the value to be added
   */
  void put(PreviewId previewId, String loggerName, String propertyName, Object value);

  /**
   * Get the preview data associated with the given program id.
   * @param previewId the id of the preview for which preview data to be fetched
   * @param loggerName the name of the logger used to log the preview data
   * @return the {@link Map} of property and associated values logged for the program
   */
  Map<String, List<String>> get(PreviewId previewId, String loggerName);

  /**
   * Removes the preview data logged by specified programId
   * @param previewId the id of the preview for which the data to be removed
   */
  void remove(PreviewId previewId);

  /**
   * Clears the preview data store.
   */
  void clear() throws IOException, DatasetManagementException;
}
