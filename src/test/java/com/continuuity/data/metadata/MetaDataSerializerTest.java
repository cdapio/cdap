package com.continuuity.data.metadata;

import com.continuuity.api.data.MetaDataEntry;
import com.continuuity.api.data.MetaDataException;
import org.junit.Assert;
import org.junit.Test;

public class MetaDataSerializerTest {

  // test that a serialize followed by deserialize returns identity
  void testOneSerDe(boolean update, String name, String type, String field,
                    String text, String binaryField, byte[] binary)
      throws MetaDataException {

    MetaDataEntry meta = new MetaDataEntry(name, type);
    if (field != null) meta.addField(field, text);
    if (binaryField != null) meta.addField(binaryField, binary);
    MetaDataSerializer serializer = new MetaDataSerializer();
    Assert.assertEquals(
        meta, serializer.deserialize(serializer.serialize(meta)));
  }

  @Test
  public void testSerializeDeserialize() throws MetaDataException {
    testOneSerDe(false, "name", "type", "a", "b", "abc", new byte[]{'x'});
    // test names and values with non-Latin characters
    testOneSerDe(false, "\0", "\u00FC", "\u1234", "", "\uFFFE", new byte[]{});
    // test text and binary fields with the same name
    testOneSerDe(false, "n", "t", "a", "b", "a", new byte[]{'x'});
  }

}
