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

package io.cdap.cdap.proto.sourcecontrol;

import java.time.Instant;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The source control metadata for an application.
 */
public class SourceControlMeta {

  private final String fileHash;
  private final String commitId;
  // The last time the application was synced(push/pull) with git.
  private final Instant lastSyncedAt;
  @Nullable
  private final Boolean syncStatus;

  /**
   * Default constructor for SourceControlMeta.
   *
   * @param fileHash     the git-hash of the config in git after push
   * @param commitId     the commit in git form/to application was pulled/pushed
   * @param lastSyncedAt last time the application was pulled/pushed
   */
  public SourceControlMeta(String fileHash, @Nullable String commitId,
      @Nullable Instant lastSyncedAt) {
    this(fileHash, commitId, lastSyncedAt, null);
  }

  /**
   * Constructs a new instance of {@code SourceControlMeta} with the specified parameters.
   *
   * @param fileHash     The hash value of the file associated with the metadata.
   * @param commitId     The ID of the commit associated with the metadata, or {@code null} if not
   *                     available.
   * @param lastSyncedAt The timestamp when the metadata was last synced, or {@code null} if not
   *                     available.
   * @param syncStatus   The synchronization status indicating whether the metadata is synchronized,
   *                     represented as a boolean value.
   */
  public SourceControlMeta(String fileHash, @Nullable String commitId,
      @Nullable Instant lastSyncedAt, @Nullable Boolean syncStatus) {
    this.fileHash = fileHash;
    this.commitId = commitId;
    this.lastSyncedAt = lastSyncedAt;
    this.syncStatus = syncStatus;
  }

  public String getFileHash() {
    return fileHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceControlMeta that = (SourceControlMeta) o;
    return Objects.equals(fileHash, that.fileHash) && Objects.equals(commitId,
        that.commitId) && Objects.equals(lastSyncedAt, that.lastSyncedAt)
        && Objects.equals(syncStatus, that.syncStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileHash, commitId, lastSyncedAt, syncStatus);
  }

  @Override
  public String toString() {
    return "SourceControlMeta{" +
        "fileHash='" + fileHash + '\'' +
        ", commitId='" + commitId + '\'' +
        ", lastSyncedAt=" + lastSyncedAt +
        ", syncStatus=" + syncStatus +
        '}';
  }

  @Nullable
  public String getCommitId() {
    return commitId;
  }

  @Nullable
  public Instant getLastSyncedAt() {
    return lastSyncedAt;
  }

  @Nullable
  public Boolean getSyncStatus() {
    return syncStatus;
  }

  public static SourceControlMeta createDefaultMeta() {
    return new SourceControlMeta(null, null, null, false);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(SourceControlMeta sourceControlMeta) {
    return new Builder().setFileHash(sourceControlMeta.getFileHash()).setCommitId(
        sourceControlMeta.getCommitId()).setLastSyncedAt(sourceControlMeta.getLastSyncedAt());
  }

  /**
   * Builds the SourceControlMeta.
   */
  public static class Builder {
    private String fileHash;
    private String commitId;
    private Instant lastSyncedAt;
    private Boolean syncStatus;

    public Builder setSyncStatus(Boolean syncStatus) {
      this.syncStatus = syncStatus;
      return this;
    }

    public Builder setCommitId(String commitId) {
      this.commitId = commitId;
      return this;
    }

    public Builder setLastSyncedAt(Instant lastSyncedAt) {
      this.lastSyncedAt = lastSyncedAt;
      return this;
    }

    public Builder setFileHash(String fileHash) {
      this.fileHash = fileHash;
      return this;
    }

    public SourceControlMeta build() {
      return new SourceControlMeta(fileHash, commitId, lastSyncedAt, syncStatus);
    }
  }

}
