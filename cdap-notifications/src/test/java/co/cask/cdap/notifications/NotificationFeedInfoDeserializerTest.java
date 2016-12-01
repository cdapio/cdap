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

package co.cask.cdap.notifications;

import co.cask.cdap.proto.notification.NotificationFeedInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test backwards compatibility.
 */
public class NotificationFeedInfoDeserializerTest {

  @Test
  public void testDeserialization() {
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(NotificationFeedInfo.class, new NotificationFeedInfoDeserializer())
      .create();

    NotificationFeedInfo newFeed = new NotificationFeedInfo("ns1", "category", "feed1", "desc");
    LegacyNotificationFeedInfo legacyFeed = new LegacyNotificationFeedInfo(newFeed);
    Assert.assertEquals(newFeed, gson.fromJson(gson.toJson(legacyFeed), NotificationFeedInfo.class));
  }

  /**
   * Same structure as legacy Id.Namespace.
   */
  @SuppressWarnings("unused")
  private static class LegacyNamespace {
    private final String id;

    public LegacyNamespace(String id) {
      this.id = id;
    }
  }

  /**
   * Same structure as legacy Id.NotificationFeed.
   */
  @SuppressWarnings("unused")
  private static class LegacyNotificationFeedInfo {
    private final LegacyNamespace namespace;
    private final String category;
    private final String name;
    private final String description;

    public LegacyNotificationFeedInfo(NotificationFeedInfo info) {
      this.namespace = new LegacyNamespace(info.getNamespace());
      this.category = info.getCategory();
      this.name = info.getFeed();
      this.description = info.getDescription();
    }
  }
}
