/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.data2.transaction.queue.hbase.coprocessor;

/**
 * Helper class for holding consumer groupId and instanceId for hash map lookup.
 *
 * NOTE: This class is not thread safe.
 */
public final class ConsumerInstance {
  private long groupId;
  private int instanceId;
  private boolean hashCodeComputed;
  private int hashCode;

  public ConsumerInstance(long groupId, int instanceId) {
    setGroupInstance(groupId, instanceId);
  }

  public void setGroupInstance(long groupId, int instanceId) {
    this.groupId = groupId;
    this.instanceId = instanceId;
    this.hashCodeComputed = false;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != ConsumerInstance.class) {
      return false;
    }
    ConsumerInstance other = (ConsumerInstance) o;
    return groupId == other.groupId && instanceId == other.instanceId;
  }

  @Override
  public int hashCode() {
    if (hashCodeComputed) {
      return hashCode;
    }

    hashCode = (int) ((groupId ^ (groupId >> 32)) ^ instanceId);
    hashCodeComputed = true;
    return hashCode;
  }
}
