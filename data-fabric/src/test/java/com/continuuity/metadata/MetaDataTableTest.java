package com.continuuity.metadata;

import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data2.OperationException;
import com.continuuity.test.SlowTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Common methods for Metadata store test.
 */
public abstract class MetaDataTableTest {

  static MetaDataTable mds;
  static OperationContext context = new OperationContext(Constants.DEVELOPER_ACCOUNT_ID);

  abstract void clearMetaData() throws OperationException;

  // test that a write followed by a read returns identity
  void testOneAddGet(boolean update, String account,
                     String application, String type, String id,
                     String field, String text,
                     String binaryField, byte[] binary)
      throws OperationException {

    MetaDataEntry meta = new MetaDataEntry(account, application, type, id);
    if (field != null) {
      meta.addField(field, text);
    }
    if (binaryField != null) {
      meta.addField(binaryField, binary);
    }
    if (update) {
      mds.update(context, meta);
    } else {
      mds.add(context, meta);
    }
    MetaDataEntry meta2 = mds.get(context, account, application, type, id);
    Assert.assertEquals(meta, meta2);
  }

  @Test
  public void testAddAndGet() throws OperationException {
    testOneAddGet(false, "a", "b", "name", "type",
        "a", "b", "abc", new byte[]{'x'});
    // test names and values with non-Latin characters
    testOneAddGet(false, "\1", null, "\0", "\u00FC",
        "\u1234", "", "\uFFFE", new byte[]{});
    // test text and binary fields with the same name
    testOneAddGet(false, "a", "b", "n", "t",
        "a", "b", "a", new byte[]{'x'});
  }

  // test that consecutive writes overwrite
  @Test
  public void testOverwrite() throws Exception {
    testOneAddGet(false, "a", null, "x", "1", "a", "b", "p", new byte[]{'q'});
    testOneAddGet(true, "a", null, "x", "1", "a", "c", "p", new byte[]{'r'});
    testOneAddGet(false, "a", "b", "x", "1", "a", "b", "p", new byte[]{'q'});
    testOneAddGet(true, "a", "b", "x", "1", "a", "c", "p", new byte[]{'r'});
  }

  // test that update with fewer columns deletes old columns
  @Test
  public void testFewerFields() throws Exception {
    testOneAddGet(false, "a", null, "y", "1", "a", "b", "p", new byte[]{'q'});
    testOneAddGet(true, "a", null, "y", "1", "a", "c", null, null);
  }

  // test that update fails if not existent
  @Test
  public void testUpdateNonExisting() throws Exception {
    try {
      testOneAddGet(true, "a", null, "z", "1", "a", "c", null, null);
      Assert.fail("expected an OperationException for updating non-existent.");
    } catch (OperationException e) {
      Assert.assertEquals(StatusCode.ENTRY_NOT_FOUND, e.getStatus());
    }
  }

  // test that insert fails if existent
  @Test
  public void testAddExisting() throws Exception {
    // add a meta data record
    MetaDataEntry meta = new MetaDataEntry("a", "a", "zz", "1");
    meta.addField("a", "c");
    mds.add(context, meta);
    // add the same record again.
    // succeeds because add() does conflict resolution
    mds.add(context, meta);
    // add the same record again, disabling conflict resolution -> fail
    try {
      mds.add(context, meta, false);
      Assert.fail("expected an OperationException for adding existing entry.");
    } catch (OperationException e) {
      Assert.assertEquals(StatusCode.ENTRY_EXISTS, e.getStatus());
    }
    // now add a modified record -> even conflict resolution must now fail
    meta.addField("new", "field");
    try {
      mds.add(context, meta);
      Assert.fail("expected an OperationException for adding existing entry.");
    } catch (OperationException e) {
      Assert.assertEquals(StatusCode.ENTRY_EXISTS, e.getStatus());
    }
  }

  // test that swap fails if the existing value matches neither the
  // expected nor the new value
  @Test
  public void testSwap() throws OperationException {
    // add a meta data record
    MetaDataEntry meta = new MetaDataEntry("a", "b", "typ", "thisid");
    meta.addField("a", "x");
    mds.add(context, meta);

    // create another entry with the same key but different field value
    MetaDataEntry meta1 = new MetaDataEntry("a", "b", "typ", "thisid");
    meta1.addField("a", "y");

    // validate that swap succeeds
    mds.swap(context, meta, meta1);

    // create another entry with the same key but different field value
    MetaDataEntry meta2 = new MetaDataEntry("a", "b", "typ", "thisid");
    meta2.addField("a", "z");
    meta2.addField("b", "1");

    // validate that swapping again fails with the old expected value
    try {
      mds.swap(context, meta, meta2);
      Assert.fail("Expected mismatch");
    } catch (OperationException e) { // expected
      Assert.assertEquals(StatusCode.ENTRY_DOES_NOT_MATCH, e.getStatus());
    }

    // update the entry to meta2, and try swapping again, should now succeed
    // although the expected value is not there, but the new value matches
    mds.update(context, meta2);
    mds.swap(context, meta, meta2);

    // create a new meta data entry and add fields in different order
    MetaDataEntry meta3 = new MetaDataEntry("a", "b", "typ", "thisid");
    meta3.addField("b", "1");
    meta3.addField("a", "z");

    // verify that swap succeeds despite the different order of fields
    mds.swap(context, meta3, meta);
  }

  @Test
  public void testList() throws OperationException {
    clearMetaData();

    testOneAddGet(false, "a", "p", "x", "1", "a", "1", null, null);
    testOneAddGet(false, "a", "p", "y", "2", "a", "2", null, null);
    testOneAddGet(false, "a", "q", "x", "3", "b", "1", null, null);
    testOneAddGet(false, "a", null, "x", "4", "a", "2", null, null);
    testOneAddGet(false, "b", null, "x", "1", "b", "2", null, null);
    testOneAddGet(false, "b", "r", "y", "2", "b", "2", null, null);

    MetaDataEntry meta1 = mds.get(context, "a", "p", "x", "1");
    MetaDataEntry meta2 = mds.get(context, "a", "p", "y", "2");
    MetaDataEntry meta3 = mds.get(context, "a", "q", "x", "3");
    MetaDataEntry meta4 = mds.get(context, "a", null, "x", "4");
    MetaDataEntry meta6 = mds.get(context, "b", "r", "y", "2");

    List<MetaDataEntry> entries;

    // list with account a for type x should yield meta1, meta3 and meta4
    entries = mds.list(context, "a", null, "x", null);
    Assert.assertEquals(3, entries.size());
    Assert.assertTrue(entries.contains(meta1));
    Assert.assertTrue(entries.contains(meta3));
    Assert.assertTrue(entries.contains(meta4));

    // list type x for account a and app p should yield only meta1
    entries = mds.list(context, "a", "p", "x", null);
    Assert.assertEquals(1, entries.size());
    Assert.assertTrue(entries.contains(meta1));

    // list type z for account b should yield nothing
    entries = mds.list(context, "b", null, "z", null);
    Assert.assertTrue(entries.isEmpty());

    // list type y for account b should yield meta6
    entries = mds.list(context, "b", null, "y", null);
    Assert.assertEquals(1, entries.size());
    Assert.assertTrue(entries.contains(meta6));

    // list all type x in acount a that have field a should yield 1,4
    Map<String, String> hasFieldA = Collections.singletonMap("a", null);
    entries = mds.list(context, "a", null, "x", hasFieldA);
    Assert.assertEquals(2, entries.size());
    Assert.assertTrue(entries.contains(meta1));
    Assert.assertTrue(entries.contains(meta4));

    // list all type y that have field a should yield only 2
    entries = mds.list(context, "a", "p", "y", hasFieldA);
    Assert.assertEquals(1, entries.size());
    Assert.assertTrue(entries.contains(meta2));

    // list all that have field a=1 should yield 1
    Map<String, String> hasFieldA1 = Collections.singletonMap("a", "1");
    entries = mds.list(context, "a", null, "x", hasFieldA1);
    Assert.assertEquals(1, entries.size());
    Assert.assertTrue(entries.contains(meta1));

    // list all accounts
    Collection<String> accounts = mds.listAccounts(new OperationContext("root"));
    List<String> sortedAccounts = new ArrayList<String>(accounts);
    Collections.sort(sortedAccounts);
    Assert.assertArrayEquals(new String[]{"a", "b"},  sortedAccounts.toArray(new String[sortedAccounts.size()]));
  }

  // test delete
  @Test
  public void testDelete() throws OperationException {
    // add an entry with a text and binary field
    MetaDataEntry meta = new MetaDataEntry("u", "q", "tbd", "whatever");
    meta.addField("text", "some text");
    meta.addField("binary", new byte[] { 'b', 'i', 'n' });
    mds.add(context, meta);

    // verify it was written
    Assert.assertEquals(meta, mds.get(context, "u", "q", "tbd", "whatever"));

    // delete it
    mds.delete(context, "u", "q", "tbd", "whatever");

    // verify it's gone
    Assert.assertNull(mds.get(context, "u", "q", "tbd", "whatever"));
    Assert.assertFalse(mds.list(context, "u", null, "tbd", null).contains(meta));

    // add another entry with same name and type
    MetaDataEntry meta1 = new MetaDataEntry("u", "q", "tbd", "whatever");
    meta1.addField("other", "other text");
    // add should succeed, update should fail
    try {
      mds.update(context, meta1);
      Assert.fail("update should fail");
    } catch (OperationException e) {
      //expected
    }
    mds.add(context, meta1);

    // read back entry and verify that it does not contain spurious
    // fields from the old meta data entry
    // verify it was written
    Assert.assertEquals(meta1, mds.get(context, "u", "q", "tbd", "whatever"));
  }

  // test clear
  @Test
  public void testClear() throws OperationException {
    testOneAddGet(false, "a", "p", "a", "x", "a", "1", null, null);
    testOneAddGet(false, "a", "q", "b", "y", "a", "2", null, null);
    testOneAddGet(false, "a", null, "c", "z", "a", "1", null, null);
    testOneAddGet(false, "b", "q", "c", "z", "a", "1", null, null);

    // clear account a, app p
    mds.clear(context, "a", "p");
    Assert.assertNull(mds.get(context, "a", "p", "a", "x"));
    Assert.assertNotNull(mds.get(context, "a", "q", "b", "y"));
    Assert.assertNotNull(mds.get(context, "a", null, "c", "z"));

    // clear all for account a
    mds.clear(context, "a", null);
    Assert.assertNull(mds.get(context, "a", "q", "b", "y"));
    Assert.assertNull(mds.get(context, "a", null, "c", "z"));

    // make sure account b is still there
    Assert.assertNotNull(mds.get(context, "b", "q", "c", "z"));
  }

  @Category(SlowTests.class)
  @Test
  public void testConcurrentUpdateField() throws Exception {
    // create a meta data entry with two fields, one text one bin
    MetaDataEntry entry = new MetaDataEntry(context.getAccount(), null, "test", "abc");
    entry.addField("text", "0");
    entry.addField("bin", Bytes.toBytes(0));
    mds.add(context, entry);

    // start two threads that loop n times
    //   - each attempts to update one of the fields
    //   - each increments the field if successful
    //   - each counts the number of write conflicts

    final AtomicInteger textConflicts = new AtomicInteger(0);
    final AtomicInteger binaryConflicts = new AtomicInteger(0);
    final int numRounds = 1000;

    Thread textThread = new Thread() {
      @Override
      public void run() {
        int value = 1;
        for (int i = 1; i <= numRounds; i++) {
          try {
            mds.updateField(context, context.getAccount(), null, "test", "abc", "text", Integer.toString(value), 0);
            value++;
          } catch (OperationException e) {
            if (e.getStatus() == StatusCode.WRITE_CONFLICT) {
              // System.err.println("Conflict for text " + i + ": " + value);
              textConflicts.incrementAndGet();
            } else {
              Assert.fail("failure for text " + i + ": " + e.getMessage());
            }
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
        }
      }
    };
    Thread binaryThread = new Thread() {
      @Override
      public void run() {
        int value = 1;
        for (int i = 1; i <= numRounds; i++) {
          try {
            mds.updateField(context, context.getAccount(), null, "test", "abc", "bin", Bytes.toBytes(value), 0);
            value++;
          } catch (OperationException e) {
            if (e.getStatus() == StatusCode.WRITE_CONFLICT) {
              // System.err.println("Conflict for binary " + i + ": " + value);
              binaryConflicts.incrementAndGet();
            } else {
              Assert.fail("failure for binary " + i + ": " + e.getMessage());
            }
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
        }
      }
    };
    textThread.start();
    binaryThread.start();

    // in the end, the values of the two fields must be incremented
    // (n - #conflicts(field)) times
    textThread.join();
    binaryThread.join();

    entry = mds.get(context, context.getAccount(), null, "test", "abc");
    Assert.assertNotNull(entry);
    String text = entry.getTextField("text");
    byte[] binary = entry.getBinaryField("bin");
    System.out.println("text: " + text + ", " + textConflicts.get() + " conflicts");
    System.out.println("binary: " + Arrays.toString(binary) + ", " + binaryConflicts.get() + " conflicts");

    Assert.assertEquals(numRounds, Integer.parseInt(text) + textConflicts.get());
    Assert.assertEquals(numRounds, Bytes.toInt(binary) + binaryConflicts.get());
  }

  class TextThread extends Thread {
    private AtomicInteger conflicts;
    public TextThread(AtomicInteger conflicts) {
      this.conflicts = conflicts;
    }
    @Override
    public void run() {
      for (int i = 1; i <= 1000; i++) {
        try {
          String old = mds.get(context, context.getAccount(), null, "test", "xyz").getTextField("num");
          int value = Integer.valueOf(old);
          mds.swapField(context, context.getAccount(), null, "test", "xyz", "num", old, Integer.toString(value + 1), 0);
        } catch (OperationException e) {
          // conflicts can happen a) within the tx, and b) as a compare-and-swap failure
          if (e.getStatus() == StatusCode.WRITE_CONFLICT || e.getStatus() == StatusCode.ENTRY_DOES_NOT_MATCH) {
            // System.out.println("Conflict for text " + i + ": " + value);
            conflicts.incrementAndGet();
          } else {
            Assert.fail(e.getMessage());
          }
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
      }
    }
  }

  class BinThread extends Thread {
    private AtomicInteger conflicts;
    public BinThread(AtomicInteger conflicts) {
      this.conflicts = conflicts;
    }
    @Override
    public void run() {
      for (int i = 1; i <= 1000; i++) {
        try {
          byte[] old = mds.get(context, context.getAccount(), null, "test", "xyz").getBinaryField("num");
          int value = Bytes.toInt(old);
          mds.swapField(context, context.getAccount(), null, "test", "xyz", "num", old, Bytes.toBytes(value + 1), 0);
        } catch (OperationException e) {
          // conflicts can happen a) within the tx, and b) as a compare-and-swap failure
          if (e.getStatus() == StatusCode.WRITE_CONFLICT || e.getStatus() == StatusCode.ENTRY_DOES_NOT_MATCH) {
            conflicts.incrementAndGet();
          } else {
            Assert.fail(e.getMessage());
          }
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
      }
    }
  }

  @Category(SlowTests.class)
  @Test
  public void testConcurrentSwapField() throws Exception {
    System.out.println("testSwapField:");
    // create a meta data entry with two fields, one text one bin
    MetaDataEntry entry =
        new MetaDataEntry(context.getAccount(), null, "test", "xyz");
    entry.addField("num", "0");
    entry.addField("num", Bytes.toBytes(0));
    mds.add(context, entry);

    // start two threads that loop n times
    //   - each attempts to update one of the fields
    //   - each increments the field if successful
    //   - each counts the number of write conflicts

    final AtomicInteger textConflicts = new AtomicInteger(0);
    final AtomicInteger binaryConflicts = new AtomicInteger(0);

    Thread textThread1 = new TextThread(textConflicts);
    Thread textThread2 = new TextThread(textConflicts);
    Thread binaryThread1 = new BinThread(binaryConflicts);
    Thread binaryThread2 = new BinThread(binaryConflicts);
    textThread1.start();
    textThread2.start();
    binaryThread1.start();
    binaryThread2.start();

    // in the end, the values of the two fields must be incremented
    // (n - #conflicts(field)) times
    textThread1.join();
    textThread2.join();
    binaryThread1.join();
    binaryThread2.join();

    entry = mds.get(context, context.getAccount(), null, "test", "xyz");
    Assert.assertNotNull(entry);
    String text = entry.getTextField("num");
    byte[] binary = entry.getBinaryField("num");
    System.out.println("text: " + text + ", " + textConflicts.get() +
        " conflicts");
    System.out.println("binary: " + Arrays.toString(binary) + ", " +
        "" + binaryConflicts.get() + " conflicts");

    Assert.assertEquals(2000, Integer.parseInt(text) + textConflicts.get());
    Assert.assertEquals(2000, Bytes.toInt(binary) + binaryConflicts.get());
  }

  class UpdateThread extends Thread {
    AtomicInteger conflicts;
    MetaDataEntry entry;
    boolean resolve;
    UpdateThread(AtomicInteger conflicts, MetaDataEntry entry, boolean resolve) {
      this.conflicts =  conflicts;
      this.entry = entry;
      this.resolve = resolve;
    }
    @Override
    public void run() {
      for (int i = 1; i <= 1000; i++) {
        try {
          mds.update(context, entry, resolve);
        } catch (OperationException e) {
          if (e.getStatus() == StatusCode.WRITE_CONFLICT) {
            // System.out.println("Conflict for run " + i);
            conflicts.incrementAndGet();
          } else {
            Assert.fail(e.getMessage());
          }
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
      }
    }
  }

  @Category(SlowTests.class)
  @Test
  public void testConcurrentUpdate() throws Exception {
    System.out.println("testConcurrentUpdate:");
    // create a meta data entry with two fields, one text one bin
    final MetaDataEntry entry =
        new MetaDataEntry(context.getAccount(), null, "test", "punk");
    entry.addField("num", "0");
    entry.addField("num", Bytes.toBytes(0));
    mds.add(context, entry);

    // start two threads that loop n times
    //   - each attempts to update one of the fields
    //   - each increments the field if successful
    //   - each counts the number of write conflicts

    final AtomicInteger conflicts = new AtomicInteger(0);

    Thread updateThread1 = new UpdateThread(conflicts, entry, true);
    Thread updateThread2 = new UpdateThread(conflicts, entry, true);
    Thread updateThread3 = new UpdateThread(conflicts, entry, true);
    updateThread1.start();
    updateThread2.start();
    updateThread3.start();
    updateThread1.join();
    updateThread2.join();
    updateThread3.join();

    // all threads write the same entry with conflict resolution
    // thus there should be no conflicts!
    System.out.println("resolve = true:  " + conflicts.get() + " conflicts");
    Assert.assertEquals(0, conflicts.get());

    updateThread1 = new UpdateThread(conflicts, entry, false);
    updateThread2 = new UpdateThread(conflicts, entry, false);
    updateThread3 = new UpdateThread(conflicts, entry, false);
    updateThread1.start();
    updateThread2.start();
    updateThread3.start();
    updateThread1.join();
    updateThread2.join();
    updateThread3.join();

    // all threads write the same entry without conflict resolution
    // thus there must be conflicts!
    System.out.println("resolve = false: " + conflicts.get() + " conflicts");
    Assert.assertTrue(conflicts.get() > 0);
  }

}
