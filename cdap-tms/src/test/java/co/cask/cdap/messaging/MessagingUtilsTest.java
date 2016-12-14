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

package co.cask.cdap.messaging;

import co.cask.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link MessagingUtils}.
 */
public class MessagingUtilsTest {

  @Test
  public void testTopicConversion() throws Exception {
    TopicId id = new TopicId("n1", "t1");
    byte[] topicBytes = MessagingUtils.toMetadataRowKey(id);
    TopicId topicId = MessagingUtils.toTopicId(topicBytes);
    Assert.assertEquals(id, topicId);
  }
}
