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
 *
 */
public class DataSchedule extends Schedule {
  /**
   * Source type, in case the {@link ScheduleType} of this schedule is {@code DATA}.
   */
  public enum SourceType { STREAM, DATASET }

  private final String sourceNamespaceId;

  private final String sourceName;

  private final SourceType sourceType;

  private final int dataTriggerMB;

  public DataSchedule(String name, String description, SourceType sourceType, String sourceNamespaceId,
                      String sourceName, int dataTriggerMB) {
    super(ScheduleType.DATA, name, description);
    this.sourceNamespaceId = sourceNamespaceId;
    this.sourceName = sourceName;
    this.sourceType = sourceType;
    this.dataTriggerMB = dataTriggerMB;
  }

  public String getSourceNamespaceId() {
    return sourceNamespaceId;
  }

  public String getSourceName() {
    return sourceName;
  }

  public SourceType getSourceType() {
    return sourceType;
  }

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

    DataSchedule schedule = (DataSchedule) o;

    if (getDescription().equals(schedule.getDescription())
      && getName().equals(schedule.getName())
      && sourceNamespaceId.equals(schedule.sourceNamespaceId)
      && sourceName.equals(schedule.sourceName)
      && sourceType.equals(schedule.sourceType)
      && dataTriggerMB == schedule.dataTriggerMB) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = 31 * result + getDescription().hashCode();
    result = 31 * result + sourceNamespaceId.hashCode();
    result = 31 * result + sourceName.hashCode();
    result = 31 * result + sourceType.hashCode();
    result = 31 * result + dataTriggerMB;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DataSchedule{");
    sb.append("name='").append(getName()).append('\'');
    sb.append(", description='").append(getDescription()).append('\'');
    sb.append(", sourceNamespaceId='").append(sourceNamespaceId).append('\'');
    sb.append(", sourceName='").append(sourceName).append('\'');
    sb.append(", sourceType='").append(sourceType).append('\'');
    sb.append(", dataTriggerMB='").append(dataTriggerMB).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
