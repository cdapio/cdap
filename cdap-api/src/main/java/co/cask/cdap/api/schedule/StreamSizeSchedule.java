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
public class StreamSizeSchedule extends Schedule implements NotificationSchedule {

  private final String streamName;

  private final int dataTriggerMB;

  private final int pollingDelay;

  public StreamSizeSchedule(String name, String description, String streamName, int dataTriggerMB, int pollingDelay) {
    super(name, description);
    this.streamName = streamName;
    this.dataTriggerMB = dataTriggerMB;
    this.pollingDelay = pollingDelay;
  }

  @Override
  public String getFeedCategory() {
    return "stream";
  }

  @Override
  public String getFeedName() {
    return String.format("%sSize", streamName);
  }

  /**
   * @return Name of the stream this {@link StreamSizeSchedule} is based on
   */
  public String getStreamName() {
    return streamName;
  }

  /**
   * @return the size of data, in MB, that a stream has to receive for the program to be run
   */
  public int getDataTriggerMB() {
    return dataTriggerMB;
  }

  /**
   * @return Delay, in minutes, after which the stream should be polled to retrieve the size of its data if
   * no notifications has been received
   */
  public int getPollingDelay() {
    return pollingDelay;
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

    if (getDescription().equals(schedule.getDescription())
      && getName().equals(schedule.getName())
      && streamName.equals(schedule.streamName)
      && dataTriggerMB == schedule.dataTriggerMB
      && pollingDelay == schedule.pollingDelay) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = getName().hashCode();
    result = 31 * result + getDescription().hashCode();
    result = 31 * result + streamName.hashCode();
    result = 31 * result + dataTriggerMB;
    result = 31 * result + pollingDelay;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DataSchedule{");
    sb.append("name='").append(getName()).append('\'');
    sb.append(", description='").append(getDescription()).append('\'');
    sb.append(", sourceName='").append(streamName).append('\'');
    sb.append(", dataTriggerMB='").append(dataTriggerMB).append('\'');
    sb.append(", pollingDelay='").append(pollingDelay).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
