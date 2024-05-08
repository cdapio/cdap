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

  /**
   * Default constructor for SourceControlMeta.
   *
   * @param fileHash the git-hash of the config in git after push
   * @param commitId the commit in git form/to application was pulled/pushed
   * @param lastSyncedAt last time the application was pulled/pushed
   */
  public SourceControlMeta(String fileHash, @Nullable String commitId, @Nullable Instant lastSyncedAt) {
    this.fileHash = fileHash;
    this.commitId = commitId;
    this.lastSyncedAt = lastSyncedAt;
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

    return Objects.equals(fileHash, that.fileHash)
        && Objects.equals(commitId, that.commitId)
        && Objects.equals(lastSyncedAt, that.lastSyncedAt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileHash, lastSyncedAt, commitId);
  }

  @Override
  public String toString() {
    return "SourceControlMeta{"
        + "fileHash='" + fileHash + '\''
        + "commitId='" + commitId + '\''
        + "lastSyncedAt='" + lastSyncedAt + '\''
        + '}';
  }

  @Nullable
  public String getCommitId() {
    return commitId;
  }

  @Nullable
  public Instant getLastSyncedAt() {
    return lastSyncedAt;
  }
}
