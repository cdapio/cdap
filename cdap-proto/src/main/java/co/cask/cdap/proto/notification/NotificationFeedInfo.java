/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.proto.notification;

import co.cask.cdap.proto.id.NotificationFeedId;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Information about a Notification Feed.
 */
public class NotificationFeedInfo extends NotificationFeedId {
  private final String description;

  public NotificationFeedInfo(String namespace, String category, String feed, @Nullable String description) {
    super(namespace, category, feed);
    this.description = description;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    NotificationFeedInfo that = (NotificationFeedInfo) o;
    return Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), description);
  }
}
