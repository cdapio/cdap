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
   * Normally this should be > 0. But can be 0 indicating that no preferences have been set on the entity.
   */
  private final long seqId;
  /**
   * Whether it is a resolved preferences or not.
   */
  private boolean resolved;

  /**
   * Return a resolved preference detail where preferences in {@code parent} take precedence over
   * those in {@code child}. The {@code seqId} would be the max of the two.
   */
  public static PreferencesDetail resolve(PreferencesDetail parent, PreferencesDetail child) {
    Map<String, String> properties = new HashMap<>();
    // Copy child's properties first.
    properties.putAll(child.getProperties());
    // Add parent's properties and overrides any existing properties in child;
    properties.putAll(parent.getProperties());

    long seqId = Long.max(parent.getSeqId(), child.getSeqId());

    return new PreferencesDetail(properties, seqId, true);
  }

  public PreferencesDetail(Map<String, String> properties, long seqId, boolean resolved) {
    this.properties = properties;
    this.seqId = seqId;
    this.resolved = resolved;
  }

  public long getSeqId() {
    return this.seqId;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public boolean getResolved() {
    return resolved;
  }

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
      Objects.equals(resolved, that.resolved);
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

