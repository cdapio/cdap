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
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

/**
 * For backwards compatibility, can deserialize either the old Id.NotificationFeed or NotificationFeedInfo jsons into
 * a NotificationFeedInfo.
 */
public class NotificationFeedInfoDeserializer implements JsonDeserializer<NotificationFeedInfo> {

  @Override
  public NotificationFeedInfo deserialize(JsonElement json, Type typeOfT,
                                          JsonDeserializationContext context) throws JsonParseException {
    JsonObject obj = json.getAsJsonObject();

    String category = obj.get("category").getAsString();
    JsonElement descriptionElement = obj.get("description");
    String description = descriptionElement == null ? "" : descriptionElement.getAsString();

    String namespace;
    String feed;
    JsonElement namespaceElement = obj.get("namespace");
    // this means its the old Id.NotificationFeed object where namespace is something like "namespace":{"id":"default"}
    if (namespaceElement.isJsonObject()) {
      namespace = namespaceElement.getAsJsonObject().get("id").getAsString();
      feed = obj.get("name").getAsString();
    } else {
      namespace = namespaceElement.getAsString();
      feed = obj.get("feed").getAsString();
    }

    return new NotificationFeedInfo(namespace, category, feed, description);
  }
}
