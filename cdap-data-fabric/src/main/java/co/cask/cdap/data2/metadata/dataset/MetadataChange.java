/*
 * Copyright 2018 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.dataset;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Represents the change in Metadata
 */
public class MetadataChange {
  private final Metadata existing;
  private final Metadata latest;

  MetadataChange(Metadata existing, Metadata latest) {
    this.existing = existing;
    this.latest = latest;
  }

  /**
   * @return Metadata before the operation
   */
  public Metadata getExisting() {
    return existing;
  }

  /**
   * @return Metadata after the operation
   */
  public Metadata getLatest() {
    return latest;
  }

  /**
   * @return Properties which were deleted during the operation
   */
  public Map<String, String> getDeletedProperties() {
    return Maps.difference(existing.getProperties(), latest.getProperties()).entriesOnlyOnLeft();
  }
}
