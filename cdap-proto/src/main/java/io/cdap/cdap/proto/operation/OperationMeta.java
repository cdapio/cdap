/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.operation;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Metadata for an operation includes 1. The resources on which operation is executed 2. Timestamp
 * of operation create 3. Timestamp of operation endtime
 */
public class OperationMeta {

  private final Set<OperationResource> resources;
  private final Instant createTime;

  @Nullable
  private final Instant endTime;

  /**
   * Default constructor for OperationMeta.
   *
   * @param resources list of resources the operation is executed
   * @param createTime timestamp when the operation was created
   * @param endTime timestamp when the operation reached an end state
   */
  private OperationMeta(Set<OperationResource> resources, Instant createTime,
      @Nullable Instant endTime) {
    this.resources = resources;
    this.createTime = createTime;
    this.endTime = endTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OperationMeta that = (OperationMeta) o;

    return this.resources.equals(that.resources)
        && Objects.equals(this.createTime, that.createTime)
        && Objects.equals(this.endTime, that.endTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resources, createTime, endTime);
  }

  public Set<OperationResource> getResources() {
    return resources;
  }

  public Instant getCreateTime() {
    return createTime;
  }

  @Nullable
  public Instant getEndTime() {
    return endTime;
  }

  /**
   * Creates a Builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Create a Builder from existing run.
   *
   * @param meta existing meta to copy fields from
   */
  public static Builder builder(OperationMeta meta) {
    return new Builder(meta);
  }

  /**
   * Builder to create OperationMeta.
   */
  @SuppressWarnings("unchecked")
  public static class Builder {

    private Set<OperationResource> resources;
    private Instant createTime;
    private Instant endTime;

    protected Builder() {
      this.resources = Collections.emptySet();
    }

    protected Builder(OperationMeta operationMeta) {
      this.resources = operationMeta.resources;
      this.createTime = operationMeta.createTime;
      this.endTime = operationMeta.endTime;
    }

    public Builder setResources(Set<OperationResource> resources) {
      this.resources = resources;
      return this;
    }

    public Builder setCreateTime(Instant createTime) {
      this.createTime = createTime;
      return this;
    }

    public Builder setEndTime(Instant endTime) {
      this.endTime = endTime;
      return this;
    }

    /**
     * Builds the OperationMeta.
     */
    public OperationMeta build() {
      if (createTime == null) {
        throw new IllegalArgumentException("create time must be specified");
      }
      return new OperationMeta(resources, createTime, endTime);
    }
  }
}
