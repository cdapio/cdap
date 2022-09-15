/*
 * Copyright © 2014-2022 Cask Data, Inc.
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

package io.cdap.cdap.api.app;

import javax.annotation.Nullable;

/**
 * Change Summary returned in the app response.
 */
public class ChangeSummary {
  @Nullable
  private final String description;
  @Nullable
  private final String author;
  @Nullable
  private final Long creationTime;
  @Nullable
  private final String isLatest;

  public ChangeSummary(@Nullable String description, @Nullable String author, @Nullable Long creationTime,
                       @Nullable String isLatest) {
    this.description = description;
    this.author = author;
    this.creationTime = creationTime;
    this.isLatest = isLatest;
  }

  /**
   * @return The creation time of the update
   */
  @Nullable
  public Long getCreationTime() {
    return creationTime;
  }

  /**
   * @return Author(user name) of the change.
   */
  @Nullable
  public String getAuthor() {
    return author;
  }

  /**
   * @return Description of the change.
   */
  @Nullable
  public String getDescription() {
    return description;
  }

  /**
   * @return If the change is the latest for the app.
   */
  @Nullable
  public String isLatest() {
    return isLatest;
  }
}
