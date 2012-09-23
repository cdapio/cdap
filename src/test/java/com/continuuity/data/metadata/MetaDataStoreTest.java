package com.continuuity.data.metadata;

import com.continuuity.api.data.MetaDataEntry;
import com.continuuity.api.data.MetaDataException;
import com.continuuity.api.data.MetaDataStore;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.executor.OperationExecutor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

abstract public class MetaDataStoreTest {

  static OperationExecutor opex;
  static MetaDataStore mds;

  // test that a write followed by a read returns identity
  void testOneAddGet(boolean update, String name, String type, String field,
                     String text, String binaryField, byte[] binary)
      throws MetaDataException {

    MetaDataEntry meta = new MetaDataEntry(name, type);
    if (field != null) meta.addField(field, text);
    if (binaryField != null) meta.addField(binaryField, binary);
    if (update) mds.update(meta); else mds.add(meta);
    MetaDataEntry meta2 = mds.get(name, type);
    Assert.assertEquals(meta, meta2);
  }

  @Test
  public void testAddAndGet() throws MetaDataException {
    testOneAddGet(false, "name", "type", "a", "b", "abc", new byte[]{'x'});
    // test names and values with non-Latin characters
    testOneAddGet(false, "\0", "\u00FC", "\u1234", "", "\uFFFE", new byte[]{});
    // test text and binary fields with the same name
    testOneAddGet(false, "n", "t", "a", "b", "a", new byte[]{'x'});
  }

  // test that consecutive writes overwrite
  @Test public void testOverwrite() throws Exception {
    testOneAddGet(false, "x", "1", "a", "b", "p", new byte[]{'q'});
    testOneAddGet(true, "x", "1", "a", "c", "p", new byte[]{'r'});
  }

  // test that update with fewer columns deletes old columns
  @Test public void testFewerFields() throws Exception {
    testOneAddGet(false, "y", "1", "a", "b", "p", new byte[]{'q'});
    testOneAddGet(true, "y", "1", "a", "c", null, null);
  }

  // test that update fails if not existent
  @Test(expected = MetaDataException.class)
  public void testUpdateNonExisting() throws Exception {
    testOneAddGet(true, "z", "1", "a", "c", null, null);
  }

  // test that insert fails if existent
  @Test(expected = MetaDataException.class)
  public void testAddExisting() throws Exception {
    testOneAddGet(false, "zz", "1", "a", "c", null, null);
    testOneAddGet(false, "zz", "1", "a", "c", null, null);
  }

  @Test
  public void testList() throws MetaDataException, OperationException {
    opex.execute(new ClearFabric(true, false, false));

    testOneAddGet(false, "t1", "x", "a", "1", null, null);
    testOneAddGet(false, "t2", "x", "a", "2", null, null);
    testOneAddGet(false, "t3", "y", "a", "1", null, null);
    testOneAddGet(false, "t4", "z", "b", "2", null, null);

    MetaDataEntry meta1 = mds.get("t1", "x");
    MetaDataEntry meta2 = mds.get("t2", "x");
    MetaDataEntry meta3 = mds.get("t3", "y");
    MetaDataEntry meta4 = mds.get("t4", "z");

    List<MetaDataEntry> entries;

    // list with type=null should yield all
    entries = mds.list((String)null);
    Assert.assertEquals(4, entries.size());
    Assert.assertTrue(entries.contains(meta1));
    Assert.assertTrue(entries.contains(meta2));
    Assert.assertTrue(entries.contains(meta3));
    Assert.assertTrue(entries.contains(meta4));

    // list type x should yield meta1 and meta2
    entries = mds.list("x");
    Assert.assertEquals(2, entries.size());
    Assert.assertTrue(entries.contains(meta1));
    Assert.assertTrue(entries.contains(meta2));

    // list type x should yield meta3
    entries = mds.list("y");
    Assert.assertEquals(1, entries.size());
    Assert.assertTrue(entries.contains(meta3));

    // list all that have field a should yield 1,2,3
    Map<String, String> hasFieldA = Collections.singletonMap("a", null);
    entries = mds.list(hasFieldA);
    Assert.assertEquals(3, entries.size());
    Assert.assertTrue(entries.contains(meta1));
    Assert.assertTrue(entries.contains(meta2));
    Assert.assertTrue(entries.contains(meta3));

    // list all type y that have field a should yield only 3
    entries = mds.list("y", hasFieldA);
    Assert.assertEquals(1, entries.size());
    Assert.assertTrue(entries.contains(meta3));

    // list all that have field a=1 should yield 1,3
    Map<String, String> hasFieldA1 = Collections.singletonMap("a", "1");
    entries = mds.list(hasFieldA1);
    Assert.assertEquals(2, entries.size());
    Assert.assertTrue(entries.contains(meta1));
    Assert.assertTrue(entries.contains(meta3));
  }

  // test delete
  @Test
  public void testDelete() throws MetaDataException {
    // add an entry with a text and binary field
    MetaDataEntry meta = new MetaDataEntry("tbd", "whatever");
    meta.addField("text", "some text");
    meta.addField("binary", new byte[] { 'b', 'i', 'n' });
    mds.add(meta);

    // verify it was written
    Assert.assertEquals(meta, mds.get("tbd", "whatever"));

    // delete it
    mds.delete("tbd", "whatever");

    // verify it's gone
    Assert.assertNull(mds.get("tbd", "whatever"));
    // TODO can't list until every implementation supports it
    // Assert.assertFalse(mds.list(null, null).contains(meta));

    // add another entry with same name and type
    MetaDataEntry meta1 = new MetaDataEntry("tbd", "whatever");
    meta1.addField("other", "other text");
    // add should succeed, update should fail
    try {
      mds.update(meta1);
      Assert.fail("update should fail");
    } catch (MetaDataException e) {
      //expected
    }
    mds.add(meta1);

    // read back entry and verify that it does not contain spurious
    // fields from the old meta data entry
    // verify it was written
    Assert.assertEquals(meta1, mds.get("tbd", "whatever"));
  }

  // test clear
  @Test
  public void testClear() throws MetaDataException {
    testOneAddGet(false, "a", "x", "a", "1", null, null);
    testOneAddGet(false, "b", "y", "a", "2", null, null);
    testOneAddGet(false, "c", "z", "a", "1", null, null);
    mds.clear();
    Assert.assertNull(mds.get("a", "x"));
    Assert.assertNull(mds.get("b", "y"));
    Assert.assertNull(mds.get("c", "z"));
    Assert.assertEquals(0, mds.list(null, null).size());
  }
}
