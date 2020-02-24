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
import java.util.Objects;

/**
 * Represent metadata of preferences
 */
public class PreferencesMetadata {

  /**
   * Sequence id of operations on the preferences
   */
  private final long seqId;

  public PreferencesMetadata(@Nullable long seqId) {
    this.seqId = seqId;
  }

  @Nullable
  public long getSeqId() {
    return this.seqId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PreferencesMetadata that = (PreferencesMetadata) o;
    return Objects.equals(seqId, that.seqId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(seqId);
  }

  @Override
  public String toString() {
    return "PreferencesMetadata{" +
      "seqId='" + seqId +
      '}';
  }
}
