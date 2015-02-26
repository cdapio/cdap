/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data2.queue;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Contains queue consumer instance configuration.
 */
public final class ConsumerConfig extends ConsumerGroupConfig {

  private final int instanceId;

  public ConsumerConfig(ConsumerGroupConfig groupConfig, int instanceId) {
    this(groupConfig.getGroupId(), instanceId, groupConfig.getGroupSize(),
         groupConfig.getDequeueStrategy(), groupConfig.getHashKey());
  }

  public ConsumerConfig(long groupId, int instanceId, int groupSize, DequeueStrategy dequeueStrategy, String hashKey) {
    super(groupId, groupSize, dequeueStrategy, hashKey);
    Preconditions.checkArgument(instanceId >= 0, "Instance ID must be >= 0.");
    Preconditions.checkArgument(instanceId < groupSize, "Instance ID must be < groupSize");
    this.instanceId = instanceId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("groupId", getGroupId())
      .add("instanceId", instanceId)
      .add("groupSize", getGroupSize())
      .add("dequeueStrategy", getDequeueStrategy())
      .add("hashKey", getHashKey())
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConsumerConfig other = (ConsumerConfig) o;
    return super.equals(other) && instanceId == other.instanceId;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), instanceId);
  }
}
