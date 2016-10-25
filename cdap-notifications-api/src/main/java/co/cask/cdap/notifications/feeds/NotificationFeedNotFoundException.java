/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.notifications.feeds;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.id.NotificationFeedId;

/**
 * Exception thrown when a {@link NotificationFeedId} object is not found.
 */
public class NotificationFeedNotFoundException extends NotFoundException {

  private final NotificationFeedId id;

  public NotificationFeedNotFoundException(NotificationFeedId id) {
    super(id);
    this.id = id;
  }

  public NotificationFeedId getId() {
    return id;
  }
}
