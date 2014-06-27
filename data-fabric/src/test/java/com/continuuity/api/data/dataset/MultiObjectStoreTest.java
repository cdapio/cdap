package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.data.dataset.DatasetCreationSpec;
import com.continuuity.data2.transaction.TransactionContext;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Object store tests.
 */
public class MultiObjectStoreTest extends DataSetTestBase {

  private static final byte[] a = { 'a' };
  private static final byte[] DEFAULT_OBJECT_STORE_COLUMN = { 'c' };

  @BeforeClass
  public static void configure() throws Exception {
    DataSet stringStore = new MultiObjectStore<String>("strings", String.class);
    DataSet pairStore = new MultiObjectStore<ImmutablePair<Integer, String>>(
      "pairs", new TypeToken<ImmutablePair<Integer, String>>() { }.getType());
    DataSet customStore = new MultiObjectStore<Custom>("customs", Custom.class);
    DataSet innerStore = new MultiObjectStore<CustomWithInner.Inner<Integer>>(
      "inners", new TypeToken<CustomWithInner.Inner<Integer>>() { }.getType());
    DataSet batchStore = new MultiObjectStore<String>("batch", String.class);
    DataSet batchTestsMultiCol = new MultiObjectStore<String>("batchTestsMultiCol", String.class);

    DataSet intStore = new IntegerStore("ints");
    DataSet multiStringStore = new MultiObjectStore<String>("multiString", String.class);

    setupInstantiator(Lists.newArrayList(stringStore, pairStore, customStore, innerStore,
                                         batchStore, intStore, multiStringStore, batchTestsMultiCol));
  }

  @Test
  public void testStringStore() {
    MultiObjectStore<String> stringStore = instantiator.getDataSet("strings");
    String string = "this is a string";
    stringStore.write(a, string);
    String result = stringStore.read(a);
    Assert.assertEquals(string, result);
  }

  @Test
  public void testPairStore() {
    MultiObjectStore<ImmutablePair<Integer, String>> pairStore = instantiator.getDataSet("pairs");
    ImmutablePair<Integer, String> pair = new ImmutablePair<Integer, String>(1, "second");
    pairStore.write(a, pair);
    ImmutablePair<Integer, String> result = pairStore.read(a);
    Assert.assertEquals(pair, result);
  }

  @Test
  public void testMultiValueStore() {
    MultiObjectStore<String> multiStringStore = instantiator.getDataSet("multiString");
    String string1 = "String1";
    String string2 = "String2";
    String string3 = "String3";
    String string4 = "String4";

    byte[] col1 = {'z'};
    byte[] col2 = {'y'};
    byte[] col3 = {'x'};
    byte[] col4 = {'w'};

    multiStringStore.write(a, col1, string1);
    multiStringStore.write(a, col2, string2);
    multiStringStore.write(a, col3, string3);
    multiStringStore.write(a, col4, string4);

    Map<byte[], String> result = multiStringStore.readAll(a);
    Assert.assertEquals(4, result.size());
  }

  @Test
  public void testCustomStore() {
    MultiObjectStore<Custom> customStore = instantiator.getDataSet("customs");
    Custom custom = new Custom(42, Lists.newArrayList("one", "two"));
    customStore.write(a, custom);
    Custom result = customStore.read(a);
    Assert.assertEquals(custom, result);
    custom = new Custom(-1, null);
    customStore.write(a, custom);
    result = customStore.read(a);
    Assert.assertEquals(custom, result);
  }

  @Test
  public void testInnerStore() {
    MultiObjectStore<CustomWithInner.Inner<Integer>> innerStore = instantiator.getDataSet("inners");
    CustomWithInner.Inner<Integer> inner = new CustomWithInner.Inner<Integer>(42, new Integer(99));
    innerStore.write(a, inner);
    CustomWithInner.Inner<Integer> result = innerStore.read(a);
    Assert.assertEquals(inner, result);
  }

  @Test
  public void testInstantiateWrongClass() throws Exception {
    // note: due to type erasure, this succeeds
    MultiObjectStore<Custom> store = instantiator.getDataSet("pairs");
    MultiObjectStore<ImmutablePair<Integer, String>> pairStore = instantiator.getDataSet("pairs");
    TransactionContext txContext = newTransaction();

    // but now it must fail with incompatible type
    Custom custom = new Custom(42, Lists.newArrayList("one", "two"));
    try {
      store.write(a, custom);
      Assert.fail("write should have failed with incompatible type");
    } catch (DataSetException e) {
      // expected
    }
    // write a correct object to the pair store
    ImmutablePair<Integer, String> pair = new ImmutablePair<Integer, String>(1, "second");
    pairStore.write(a, pair); // should succeed
    commitTransaction(txContext);
    // now try to read that as a custom object, should fail with class cast
    txContext = newTransaction();
    try {
      custom = store.read(a);
      Assert.fail("read should have failed with class cast exception");
    } catch (ClassCastException e) {
      // only this exception is expected (read will return a pair, but the assignment implicitly casts).
    }
  }

  @Test
  public void testCantOperateWithoutDelegate() throws UnsupportedTypeException {
    MultiObjectStore<Integer> store = new MultiObjectStore<Integer>("xyz", Integer.class);
    try {
      store.read(a);
      Assert.fail("Read should throw an exception when called before runtime");
    } catch (IllegalStateException e) {
      // expected
    }
    try {
      store.write(a, 1);
      Assert.fail("Write should throw an exception when called before runtime");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testWithCustomClassLoader() {

    // create a dummy class loader that records the name of the class it loaded
    final AtomicReference<String> lastClassLoaded = new AtomicReference<String>(null);
    ClassLoader loader = new ClassLoader() {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        lastClassLoaded.set(name);
        return super.loadClass(name);
      }
    };
    // create an instantiator that uses the dummy class loader
    DataSetInstantiator inst = new DataSetInstantiator(fabric, datasetFramework, CConfiguration.create(), loader);
    inst.setDataSets(specs, Collections.<DatasetCreationSpec>emptyList());
    // use that instantiator to get a data set instance
    inst.getDataSet("customs");
    // verify the class name was recorded (the dummy class loader was used).
    Assert.assertEquals(Custom.class.getName(), lastClassLoaded.get());
  }

  @Test
  public void testBatchReads() throws Exception {
    MultiObjectStore<String> t = instantiator.getDataSet("batch");

    // start a transaction
    TransactionContext txContext = newTransaction();
    // write 1000 random values to the table and remember them in a set
    SortedSet<Long> keysWritten = Sets.newTreeSet();
    Random rand = new Random(451);
    for (int i = 0; i < 1000; i++) {
      long keyLong = rand.nextLong();
      byte[] key = Bytes.toBytes(keyLong);
      t.write(key, Long.toString(keyLong));
      keysWritten.add(keyLong);
    }
    // commit transaction
    commitTransaction(txContext);

    // start a sync transaction
    txContext = newTransaction();
    // get the splits for the table
    List<Split> splits = t.getSplits();
    // read each split and verify the keys
    SortedSet<Long> keysToVerify = Sets.newTreeSet(keysWritten);
    verifySplits(t, splits, keysToVerify);

    commitTransaction(txContext);

    // start a sync transaction
    txContext = newTransaction();
    // get specific number of splits for a subrange
    keysToVerify = Sets.newTreeSet(keysWritten.subSet(0x10000000L, 0x40000000L));
    splits = t.getSplits(5, Bytes.toBytes(0x10000000L), Bytes.toBytes(0x40000000L));
    Assert.assertTrue(splits.size() <= 5);
    // read each split and verify the keys
    verifySplits(t, splits, keysToVerify);

    commitTransaction(txContext);
  }

  // helper to verify that the split readers for the given splits return exactly a set of keys
  private void verifySplits(MultiObjectStore<String> t, List<Split> splits, SortedSet<Long> keysToVerify)
    throws InterruptedException {
    // read each split and verify the keys, remove all read keys from the set
    for (Split split : splits) {
      SplitReader<byte[], Map<byte[], String>> reader = t.createSplitReader(split);
      reader.initialize(split);
      while (reader.nextKeyValue()) {
        byte[] key = reader.getCurrentKey();
        Map<byte[], String> values = reader.getCurrentValue();
        Assert.assertEquals(1, values.size());
        String value = values.get(DEFAULT_OBJECT_STORE_COLUMN);
//        // verify each row has the column written
        Assert.assertEquals(Long.toString(Bytes.toLong(key)), value);
        Assert.assertTrue(keysToVerify.remove(Bytes.toLong(key)));
      }
    }
    // verify all keys have been read
    if (!keysToVerify.isEmpty()) {
      System.out.println("Remaining [" + keysToVerify.size() + "]: " + keysToVerify);
    }
    Assert.assertTrue(keysToVerify.isEmpty());
  }


  @Test
  public void testBatchReadMultipleColumns() throws Exception {
    MultiObjectStore<String> t = instantiator.getDataSet("batchTestsMultiCol");
    byte [] col1 = Bytes.toBytes("c1");
    byte [] col2 = Bytes.toBytes("c2");

    // start a transaction
    TransactionContext txContext = newTransaction();
    // write 1000 random values to the table and remember them in a set
    SortedSet<Integer> keysWritten = Sets.newTreeSet();
    Random rand = new Random(451);
    for (int i = 0; i < 1000; i++) {
      int keyInt = rand.nextInt();
      byte[] key = Bytes.toBytes(keyInt);
      //write two columns
      t.write(key, col1, Long.toString(keyInt));
      t.write(key, col2, Long.toString(keyInt * 2));

      keysWritten.add(keyInt);
    }
    // commit transaction
    commitTransaction(txContext);

    // start a sync transaction
    txContext = newTransaction();
    // get the splits for the table
    List<Split> splits = t.getSplits();
    // read each split and verify the keys
    SortedSet<Integer> keysToVerify = Sets.newTreeSet(keysWritten);

    for (Split split : splits) {
      SplitReader<byte[], Map<byte[], String>> reader = t.createSplitReader(split);
      reader.initialize(split);
      while (reader.nextKeyValue()) {
        byte[] key = reader.getCurrentKey();
        Map<byte[], String> values = reader.getCurrentValue();
        Assert.assertEquals(2, values.size());
        String value1 = values.get(col1);
        String value2 = values.get(col2);

        // verify each row has the two columns written
        Assert.assertEquals(Integer.toString(Bytes.toInt(key)), value1);
        Assert.assertEquals(Integer.toString(Bytes.toInt(key) * 2), value2);
        Assert.assertTrue(keysToVerify.remove(Bytes.toInt(key)));
      }
    }

    commitTransaction(txContext);

  }

  @Test
  public void testSubclass() {
    IntegerStore ints = instantiator.getDataSet("ints");
    ints.write(42, 101);
    Assert.assertEquals((Integer) 101, ints.read(42));
  }

}
