/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import io.cdap.cdap.proto.id.ApplicationId;

/**
 * Class representing the preview data
 */
public class PreviewDataPayload {
  private final ApplicationId applicationId;
  private final String tracerName;
  private final String propertyName;
  private final Object propertyValue;

  public PreviewDataPayload(ApplicationId applicationId, String tracerName, String propertyName, Object propertyValue) {
    this.applicationId = applicationId;
    this.tracerName = tracerName;
    this.propertyName = propertyName;
    this.propertyValue = propertyValue;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }

  public String getTracerName() {
    return tracerName;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public Object getPropertyValue() {
    return propertyValue;
  }
}
