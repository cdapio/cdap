package com.continuuity.metadata;

import org.junit.Assert;
import org.junit.Test;

/**
 * Meta data entry test.
 */
public class MetaDataEntryTest {

  void testOne(String account, String application, String type, String id,
               String field, String text, String binaryField, byte[] binary) {
    MetaDataEntry meta = new MetaDataEntry(account, application, type, id);
    Assert.assertNull(meta.getTextField(field));
    Assert.assertNull(meta.getBinaryField(binaryField));
    Assert.assertEquals(0, meta.getTextFields().size());
    Assert.assertEquals(0, meta.getBinaryFields().size());
    meta.addField(field, text);
    meta.addField(binaryField, binary);
    Assert.assertEquals(id, meta.getId());
    Assert.assertEquals(type, meta.getType());
    Assert.assertEquals(application, meta.getApplication());
    Assert.assertEquals(account, meta.getAccount());
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
    testOne("a", "b", "name", "type", "a", "b", "abc", new byte[] { 'x' });
    // test names and values with non-Latin characters
    testOne("\1", null, "\0", "\u00FC", "\u1234", "", "\uFFFE", new byte[] { });
    // test text and binary fields with the same name
    testOne("a", "b", "name", "type", "a", "b", "a", new byte[] { 'x' });
  }

  // test that null values are not accepted
  @Test(expected = IllegalArgumentException.class)
  public void testNullAccount() {
    new MetaDataEntry(null, "some app", "some type", "some id");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullType() {
    new MetaDataEntry("some account", "some app", null, "some id");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullId() {
    new MetaDataEntry("some account", "some app", "some type", null);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyAccount() {
    new MetaDataEntry("", "some app", "some type", "some id");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyApplication() {
    new MetaDataEntry("some account", "", "some type", "some id");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyType() {
    new MetaDataEntry("some account", "some app", "", "some id");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyId() {
    new MetaDataEntry("some account", "some app", "some type", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullTextField() {
    new MetaDataEntry("some account", "soma app", "some type", "some id")
        .addField(null, "some value");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullBinaryField() {
    new MetaDataEntry("some account", "soma app", "some type", "some id")
        .addField(null, new byte[0]);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyTextField() {
    new MetaDataEntry("some account", "soma app", "some type", "some id")
        .addField("", "some value");
  }
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyBinaryField() {
    new MetaDataEntry("some account", "soma app", "some type", "some id")
        .addField("", new byte[1]);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullTextValue() {
    new MetaDataEntry("some account", "soma app", "some type", "some id")
        .addField("f", (String) null);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testNullBinaryValue() {
    new MetaDataEntry("some account", "soma app", "some type", "some id")
        .addField("f", (byte[]) null);
  }
  @Test
  public void testEmptyValue() {
    MetaDataEntry entry = new MetaDataEntry(
        "some account", "soma app", "some type", "some id");
    entry.addField("f", "");
    entry.addField("f", new byte[0]);
  }

  @Test
  public void testEquals() {
    MetaDataEntry meta1 = new MetaDataEntry("x", "y", "z", "v");
    Assert.assertEquals(meta1, meta1);
    Assert.assertFalse(meta1.equals(new MetaDataEntry("q", "y", "z", "v")));
    Assert.assertFalse(meta1.equals(new MetaDataEntry("x", "q", "z", "v")));
    Assert.assertFalse(meta1.equals(new MetaDataEntry("x", null, "z", "v")));
    Assert.assertFalse(meta1.equals(new MetaDataEntry("x", "y", "q", "v")));
    Assert.assertFalse(meta1.equals(new MetaDataEntry("x", "y", "z", "q")));
    MetaDataEntry meta2 = new MetaDataEntry("x", "y", "z", "v");
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
