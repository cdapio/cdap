/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.schedule;

/**
 * Defines a schedule based on data availability in a stream for running a program.
 */
public class StreamSizeSchedule implements NotificationSchedule {

  private final String name;

  private final String description;

  private final String sourceNamespaceId;

  private final String sourceName;

  private final int dataTriggerMB;

  public StreamSizeSchedule(String name, String description, String sourceNamespaceId, String sourceName,
                            int dataTriggerMB) {
    this.name = name;
    this.description = description;
    this.sourceNamespaceId = sourceNamespaceId;
    this.sourceName = sourceName;
    this.dataTriggerMB = dataTriggerMB;
  }

  @Override
  public String getNotificationSourceNamespaceId() {
    return sourceNamespaceId;
  }

  @Override
  public String getNotificationSourceName() {
    return sourceName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  /**
   * @return the size of data, in MB, that a stream has to receive for the program to be run
   */
  public int getDataTriggerMB() {
    return dataTriggerMB;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StreamSizeSchedule schedule = (StreamSizeSchedule) o;

    if (description.equals(description)
      && name.equals(name)
      && sourceNamespaceId.equals(schedule.sourceNamespaceId)
      && sourceName.equals(schedule.sourceName)
      && dataTriggerMB == schedule.dataTriggerMB) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + description.hashCode();
    result = 31 * result + sourceNamespaceId.hashCode();
    result = 31 * result + sourceName.hashCode();
    result = 31 * result + dataTriggerMB;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DataSchedule{");
    sb.append("name='").append(name).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", sourceNamespaceId='").append(sourceNamespaceId).append('\'');
    sb.append(", sourceName='").append(sourceName).append('\'');
    sb.append(", dataTriggerMB='").append(dataTriggerMB).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
