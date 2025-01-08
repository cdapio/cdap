/*
 *Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.stackdriver.logs;

import java.util.Collections;
import java.util.List;

public class LogsMapping {

  private final String resourceType;
  private final List<LabelMapping> resourceLabels;
  private final String logName;
  private final List<LabelMapping> logLabels;

  public LogsMapping(String resourceType, List<LabelMapping> resourceLabels, String logName,
      List<LabelMapping> logLabels) {
    this.resourceType = resourceType;
    this.resourceLabels = resourceLabels;
    this.logLabels = logLabels;
    this.logName = logName;
  }

  public String getResourceType() {
    return resourceType;
  }

  public String getLogName() {
    return logName;
  }

  public List<LabelMapping> getResourceLabels() {
    return resourceLabels == null ? Collections.emptyList()
        : Collections.unmodifiableList(resourceLabels);
  }

  public List<LabelMapping> getMetricLabels() {
    return logLabels == null ? Collections.emptyList()
        : Collections.unmodifiableList(logLabels);
  }


  @Override
  public String toString() {
    return "LogsMapping{resourceType=" + resourceType
        + ", resourceLabels=" + resourceLabels
        + ", logLabels=" + logLabels
        + ", logName=" + logName
        + '}';
  }
}
