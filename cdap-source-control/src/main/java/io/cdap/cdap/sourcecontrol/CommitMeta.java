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

package io.cdap.cdap.sourcecontrol;

import java.util.Objects;

/**
 * A class that holds metadata for a new commit.
 */
public class CommitMeta {

  private final String author;
  private final String committer;
  private final long timestampMillis;
  private final String message;

  public CommitMeta(String author, String committer, long timestampMillis, String message) {
    this.author = author;
    this.committer = committer;
    this.timestampMillis = timestampMillis;
    this.message = message;
  }

  public String getAuthor() {
    return author;
  }

  public String getCommitter() {
    return committer;
  }

  public long getTimestampMillis() {
    return timestampMillis;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommitMeta)) {
      return false;
    }
    CommitMeta that = (CommitMeta) o;
    return timestampMillis == that.timestampMillis && author.equals(that.author)
        && committer.equals(that.committer)
        && message.equals(that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(author, committer, timestampMillis, message);
  }
}
