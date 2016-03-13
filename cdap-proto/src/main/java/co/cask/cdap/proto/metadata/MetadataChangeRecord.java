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

package co.cask.cdap.proto.metadata;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a Metadata change for a given {@link Id.NamespacedId}, including its previous state, the change,
 * the time that the change occurred and (optionally) the entity that made the update.
 *
 * @deprecated Use {@link AuditMessage} with {@link AuditType#METADATA_CHANGE} instead.
 */
@Deprecated
public final class MetadataChangeRecord {
  private final MetadataRecord previous;
  private final MetadataDiffRecord changes;
  private final long updateTime;
  private final String updater;

  public MetadataChangeRecord(MetadataRecord previous, MetadataDiffRecord changes,
                              long updateTime) {
    this(previous, changes, updateTime, null);
  }

  public MetadataChangeRecord(MetadataRecord previous, MetadataDiffRecord changes,
                              long updateTime, @Nullable String updater) {
    this.previous = previous;
    this.changes = changes;
    this.updateTime = updateTime;
    this.updater = updater;
  }

  public MetadataRecord getPrevious() {
    return previous;
  }

  public MetadataDiffRecord getChanges() {
    return changes;
  }

  public long getUpdateTime() {
    return updateTime;
  }

  @Nullable
  public String getUpdater() {
    return updater;
  }

  /**
   * Represents the changes between the previous and the new record
   */
  public static final class MetadataDiffRecord {
    private final MetadataRecord additions;
    private final MetadataRecord deletions;

    public MetadataDiffRecord(MetadataRecord additions, MetadataRecord deletions) {
      this.additions = additions;
      this.deletions = deletions;
    }

    public MetadataRecord getAdditions() {
      return additions;
    }

    public MetadataRecord getDeletions() {
      return deletions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      MetadataDiffRecord that = (MetadataDiffRecord) o;

      return Objects.equals(additions, that.additions) &&
        Objects.equals(deletions, that.deletions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(additions, deletions);
    }

    @Override
    public String toString() {
      return "MetadataDiffRecord{" +
        "additions=" + additions +
        ", deletions=" + deletions +
        '}';
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetadataChangeRecord that = (MetadataChangeRecord) o;

    return Objects.equals(previous, that.previous) &&
      Objects.equals(changes, that.changes) &&
      updateTime == that.updateTime &&
      Objects.equals(updater, that.updater);
  }

  @Override
  public int hashCode() {
    return Objects.hash(previous, changes, updateTime, updater);
  }

  @Override
  public String toString() {
    return "MetadataChangeRecord{" +
      "previous=" + previous +
      ", changes=" + changes +
      ", updateTime=" + updateTime +
      ", updater='" + updater + '\'' +
      '}';
  }
}
