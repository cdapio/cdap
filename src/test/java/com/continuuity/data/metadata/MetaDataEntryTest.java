package com.continuuity.data.metadata;

import com.continuuity.api.data.MetaDataEntry;
import org.junit.Assert;
import org.junit.Test;

public class MetaDataEntryTest {

  void testOne(String name, String type, String field,
               String text, String binaryField, byte[] binary) {
    MetaDataEntry meta = new MetaDataEntry(name, type);
    Assert.assertNull(meta.getTextField(field));
    Assert.assertNull(meta.getBinaryField(binaryField));
    Assert.assertEquals(0, meta.getTextFields().size());
    Assert.assertEquals(0, meta.getBinaryFields().size());
    meta.addField(field, text);
    meta.addField(binaryField, binary);
    Assert.assertEquals(name, meta.getName());
    Assert.assertEquals(type, meta.getType());
    Assert.assertEquals(text, meta.getTextField(field));
    Assert.assertArrayEquals(binary, meta.getBinaryField(binaryField));
    if (!field.equals(binaryField)) {
      Assert.assertNull(meta.getTextField(binaryField));
      Assert.assertNull(meta.getBinaryField(field));
    }
    Assert.assertEquals(1, meta.getTextFields().size());
    Assert.assertEquals(1, meta.getBinaryFields().size());
    Assert.assertTrue(meta.getTextFields().contains(field));
    Assert.assertTrue(meta.getBinaryFields().contains(binaryField));
  }

  // test that adding entries works
  @Test
  public void testAddAndGet() {
    testOne("name", "type", "a", "b", "abc", new byte[] { 'x' });
    // test names and values with non-Latin characters
    testOne("\0", "\u00FC", "\u1234", "", "\uFFFE", new byte[] { });
    // test text and binary fields with the same name
    testOne("name", "type", "a", "b", "a", new byte[] { 'x' });
  }

  // test that null values are not accepted
  @Test(expected = IllegalArgumentException.class)
  public void testNullName() {
    new MetaDataEntry(null, "some type");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullType() {
    new MetaDataEntry("some name", null);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyName() {
    new MetaDataEntry("", "some type");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyType() {
    new MetaDataEntry("some name", "");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullTextField() {
    new MetaDataEntry("some name", "some type").addField(null, "some value");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullBinaryField() {
    new MetaDataEntry("some name", "some type").addField(null, new byte[0]);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTextField() {
    new MetaDataEntry("some name", "some type").addField("", "some value");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyBinaryField() {
    new MetaDataEntry("some name", "some type").addField("", new byte[1]);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullTextValue() {
    new MetaDataEntry("some name", "some type").addField("f", (String)null);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullBinaryValue() {
    new MetaDataEntry("some name", "some type").addField("f", (byte[]) null);
  }
  @Test
  public void testEmptyValue() {
    MetaDataEntry entry = new MetaDataEntry("some name", "some type");
    entry.addField("f", "");
    entry.addField("f", new byte[0]);
  }

  @Test
  public void testEquals() {
    MetaDataEntry meta1 = new MetaDataEntry("x", "y");
    Assert.assertEquals(meta1, meta1);
    Assert.assertFalse(meta1.equals(new MetaDataEntry("x", "z")));
    Assert.assertFalse(meta1.equals(new MetaDataEntry("z", "y")));
    MetaDataEntry meta2 = new MetaDataEntry("x", "y");
    Assert.assertEquals(meta1, meta2);
    meta1.addField("a", "1");
    meta2.addField("b", "2");
    Assert.assertFalse(meta1.equals(meta2));
    Assert.assertFalse(meta2.equals(meta1));
    meta1.addField("b", "2");
    meta2.addField("a", "1");
    Assert.assertEquals(meta1, meta2);
    meta1.addField("a", new byte[] { 1 });
    meta2.addField("b", new byte[] { 2 });
    Assert.assertFalse(meta1.equals(meta2));
    Assert.assertFalse(meta2.equals(meta1));
    meta1.addField("b", new byte[] { 2 });
    meta2.addField("a", new byte[] { 1 });
    Assert.assertEquals(meta1, meta2);
  }

}
