/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.metadata;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class MetaDataSerializerTest {

  // test that a serialize followed by deserialize returns identity
  void testOneSerDe(boolean update, String account, String application,
                    String name, String type, String field, String text,
                    String binaryField, byte[] binary) {

    MetaDataEntry meta = new MetaDataEntry(account, application, name, type);
    if (field != null) {
      meta.addField(field, text);
    }
    if (binaryField != null) {
      meta.addField(binaryField, binary);
    }
    MetaDataSerializer serializer = new MetaDataSerializer();
    Assert.assertEquals(
        meta, serializer.deserialize(serializer.serialize(meta)));
  }

  @Test
  public void testSerializeDeserialize() {
    testOneSerDe(false, "a", "b", "name", "type", "a", "b", "abc",
        new byte[]{'x'});
    // test names and values with non-Latin characters
    testOneSerDe(false, "\1", null, "\0", "\u00FC", "\u1234", "", "\uFFFE",
        new byte[]{});
    // test text and binary fields with the same name
    testOneSerDe(false, "a", "b", "n", "t", "a", "b", "a", new byte[]{'x'});
  }

}
