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

package io.cdap.cdap.proto;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represent preferences
 */
public class PreferencesDetail {
  private final Map<String, String> properties;
  /**
   * Sequence id of operations on the preferences
   */
  private final Long seqId;
  /**
   * Whether it is a resolved preferences or not.
   */
  private boolean resolved;

  public static PreferencesDetail merge(PreferencesDetail left, PreferencesDetail right) {
    if (left == null && right == null) {
      return null;
    } else if (left == null) {
      return right;
    } else if (right == null) {
      return left;
    }

    Map<String, String> properties = new HashMap<>();
    properties.putAll(left.getProperties());
    properties.putAll(right.getProperties());

    Long seqId = null;
    Long leftSeqId = left.getSeqId();
    Long rightSeqId = right.getSeqId();
    if (leftSeqId != null && rightSeqId != null) {
      seqId = Long.max(leftSeqId.longValue(), rightSeqId.longValue());
    } else if (leftSeqId != null) {
      seqId = new Long(leftSeqId.longValue());
    } else if (rightSeqId != null) {
      seqId = new Long(rightSeqId.longValue());
    }

    boolean resolved = (left.resolved || right.resolved);
    return new PreferencesDetail(properties, seqId, resolved);
  }

  public PreferencesDetail(Map<String, String> properties, @Nullable Long seqId, boolean resolved) {
    this.properties = properties;
    this.seqId = seqId;
    this.resolved = resolved;
  }

  @Nullable
  public Long getSeqId() {
    return this.seqId;
  }

  public Map<String, String> getProperties() { return properties; }

  public boolean getResolved() { return resolved; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PreferencesDetail that = (PreferencesDetail) o;
    return Objects.equals(properties, that.properties) &&
      Objects.equals(seqId, that.seqId) &&
      Objects.equals(resolved,that.resolved);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, seqId, resolved);
  }

  @Override
  public String toString() {
    return "PreferencesDetail{" +
      "properties='" + properties.toString() +
      "seqId='" + seqId +
      "resolved='" + resolved +
      '}';
  }
}

