/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.proto.artifact;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Change Summary returned in the app response.
 */
public class ChangeDetail extends ChangeSummary {

  @Nullable
  private final String author;
  private final long creationTimeMillis;
  @Nullable
  private final Boolean latest;

  public ChangeDetail(@Nullable String description, @Nullable String parentVersion,
      @Nullable String author,
      long creationTimeMillis, @Nullable Boolean latest) {
    super(description, parentVersion);
    this.author = author;
    this.creationTimeMillis = creationTimeMillis;
    this.latest = latest;
  }

  public ChangeDetail(@Nullable String description, @Nullable String parentVersion,
      @Nullable String author,
      long creationTimeMillis) {
    this(description, parentVersion, author, creationTimeMillis, null);
  }

  /**
   * @return The creation time of the update in millis seconds.
   */
  public long getCreationTimeMillis() {
    return creationTimeMillis;
  }

  /**
   * @return Author(user name) of the change.
   */
  @Nullable
  public String getAuthor() {
    return author;
  }

  /**
   * @return the latest value (true/false) for the app.
   */
  @Nullable
  public Boolean getLatest() {
    return latest;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ChangeDetail that = (ChangeDetail) o;

    return creationTimeMillis == that.creationTimeMillis
        && Objects.equals(author, that.author);
  }

  @Override
  public int hashCode() {
    return Objects.hash(author, creationTimeMillis);
  }

  @Override
  public String toString() {
    return "ChangeDetail{"
        + "author='" + author + '\''
        + ", creationTimeMillis=" + creationTimeMillis + '\''
        + ", latest=" + latest
        + '}';
  }
}
