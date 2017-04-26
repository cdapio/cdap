/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.proto.codec;

import co.cask.cdap.proto.Notification;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test case for serializing and deserializing {@link Notification}.
 */
public class NotificationCodecTest {

  @Test
  public void test() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key.1", "value.1");
    properties.put("key.2", "value.2");

    Gson gson = new Gson();
    Notification notification = new Notification(Notification.Type.TIME, properties);
    String serialized = gson.toJson(notification);
    Notification deserializedNotification = gson.fromJson(serialized, Notification.class);
    Assert.assertEquals(notification, deserializedNotification);
  }
}
