package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Object store tests.
 */
public class ObjectStoreTest extends DataSetTestBase {

  private static final byte[] a = { 'a' };

  @BeforeClass
  public static void configure() throws Exception {
    DataSet stringStore = new ObjectStore<String>("strings", String.class);
    DataSet pairStore = new ObjectStore<ImmutablePair<Integer, String>>(
      "pairs", new TypeToken<ImmutablePair<Integer, String>>(){}.getType());
    DataSet customStore = new ObjectStore<Custom>("customs", Custom.class);
    DataSet innerStore = new ObjectStore<CustomWithInner.Inner<Integer>>(
      "inners", new TypeToken<CustomWithInner.Inner<Integer>>(){}.getType());
    DataSet batchStore = new ObjectStore<String>("batch", String.class);
    DataSet intStore = new IntegerStore("ints");
    setupInstantiator(Lists.newArrayList(stringStore, pairStore, customStore, innerStore, batchStore, intStore));
    // this test runs all operations synchronously
    newTransaction(Mode.Sync);
  }

  @Test
  public void testStringStore() throws OperationException {
    ObjectStore<String> stringStore = instantiator.getDataSet("strings");
    String string = "this is a string";
    stringStore.write(a, string);
    String result = stringStore.read(a);
    Assert.assertEquals(string, result);
  }

  @Test
  public void testPairStore() throws OperationException {
    ObjectStore<ImmutablePair<Integer, String>> pairStore = instantiator.getDataSet("pairs");
    ImmutablePair<Integer, String> pair = new ImmutablePair<Integer, String>(1, "second");
    pairStore.write(a, pair);
    ImmutablePair<Integer, String> result = pairStore.read(a);
    Assert.assertEquals(pair, result);
  }

  @Test
  public void testCustomStore() throws OperationException {
    ObjectStore<Custom> customStore = instantiator.getDataSet("customs");
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
  public void testInnerStore() throws OperationException {
    ObjectStore<CustomWithInner.Inner<Integer>> innerStore = instantiator.getDataSet("inners");
    CustomWithInner.Inner<Integer> inner = new CustomWithInner.Inner<Integer>(42, new Integer(99));
    innerStore.write(a, inner);
    CustomWithInner.Inner<Integer> result = innerStore.read(a);
    Assert.assertEquals(inner, result);
  }

  @Test
  public void testInstantiateWrongClass() throws Exception {
    // note: due to type erasure, this succeeds
    ObjectStore<Custom> store = instantiator.getDataSet("pairs");
    // but now it must fail with incompatible type
    Custom custom = new Custom(42, Lists.newArrayList("one", "two"));
    try {
      store.write(a, custom);
      Assert.fail("write should have failed with incompatible type");
    } catch (OperationException e) {
      if (e.getStatus() != StatusCode.INCOMPATIBLE_TYPE) {
        throw e; // only incompatible type is expected
      }
    }
    // write a correct object to the pair store
    ObjectStore<ImmutablePair<Integer, String>> pairStore = instantiator.getDataSet("pairs");
    ImmutablePair<Integer, String> pair = new ImmutablePair<Integer, String>(1, "second");
    pairStore.write(a, pair); // should succeed
    // now try to read that as a custom object, should fail with class cast
    try {
      custom = store.read(a);
      Assert.fail("write should have failed with class cast exception");
    } catch (ClassCastException e) {
      // only this exception is expected (read will return a pair, but the assignment implicitly casts).
    }
  }

  @Test
  public void testCantOperateWithoutDelegate() throws OperationException, UnsupportedTypeException {
    ObjectStore<Integer> store = new ObjectStore<Integer>("xyz", Integer.class);
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
    DataSetInstantiator inst = new DataSetInstantiator(fabric, PROXY, loader);
    inst.setDataSets(specs);
    // use that instantiator to get a data set instance
    inst.getDataSet("customs");
    // verify the class name was recorded (the dummy class loader was used).
    Assert.assertEquals(Custom.class.getName(), lastClassLoaded.get());
  }

  @Test
  public void testBatchReads() throws OperationException, InterruptedException {
    ObjectStore<String> t = instantiator.getDataSet("batch");

    // start a transaction
    newTransaction(DataSetTestBase.Mode.Smart);
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
    commitTransaction();

    // start a sync transaction
    newTransaction(DataSetTestBase.Mode.Sync);
    // get the splits for the table
    List<Split> splits = t.getSplits();
    // read each split and verify the keys
    SortedSet<Long> keysToVerify = Sets.newTreeSet(keysWritten);
    verifySplits(t, splits, keysToVerify);

    // start a sync transaction
    newTransaction(DataSetTestBase.Mode.Sync);
    // get specific number of splits for a subrange
    keysToVerify = Sets.newTreeSet(keysWritten.subSet(0x10000000L, 0x40000000L));
    splits = t.getSplits(5, Bytes.toBytes(0x10000000L), Bytes.toBytes(0x40000000L));
    Assert.assertTrue(splits.size() <= 5);
    // read each split and verify the keys
    verifySplits(t, splits, keysToVerify);
  }

  // helper to verify that the split readers for the given splits return exactly a set of keys
  private void verifySplits(ObjectStore<String> t, List<Split> splits, SortedSet<Long> keysToVerify)
    throws OperationException, InterruptedException {
    // read each split and verify the keys, remove all read keys from the set
    for (Split split : splits) {
      SplitReader<byte[], String> reader = t.createSplitReader(split);
      reader.initialize(split);
      while (reader.nextKeyValue()) {
        byte[] key = reader.getCurrentKey();
        String value = reader.getCurrentValue();
        // verify each row has the two columns written
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
  public void testSubclass() throws OperationException {
    IntegerStore ints = instantiator.getDataSet("ints");
    ints.write(42, 101);
    Assert.assertEquals((Integer) 101, ints.read(42));
  }

}

class Custom {
  int i;
  ArrayList<String> sl;
  Custom(int i, ArrayList<String> sl) {
    this.i = i;
    this.sl = sl;
  }
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || o.getClass() != this.getClass()) {
      return false;
    }
    if (this.i != ((Custom) o).i) {
      return false;
    }
    if (this.sl == null) {
      return ((Custom) o).sl == null;
    }
    return this.sl.equals(((Custom) o).sl);
  }
  @Override
  public int hashCode() {
    return 31 * i + (sl != null ? sl.hashCode() : 0);
  }
}

class CustomWithInner<T> {
  T a;
  CustomWithInner(T t) {
    this.a = t;
  }
  public static class Inner<U> {
    SortedSet<Integer> set;
    U x;
    Inner(int i, U u) {
      this.set = Sets.newTreeSet();
      this.set.add(i);
      this.x = u;
    }
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Inner inner = (Inner) o;
      if (set != null ? !set.equals(inner.set) : inner.set != null) {
        return false;
      }
      if (x != null ? !x.equals(inner.x) : inner.x != null) {
        return false;
      }
      return true;
    }
    @Override
    public int hashCode() {
      int result = set != null ? set.hashCode() : 0;
      result = 31 * result + (x != null ? x.hashCode() : 0);
      return result;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CustomWithInner that = (CustomWithInner) o;
    if (a != null ? !a.equals(that.a) : that.a != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return a != null ? a.hashCode() : 0;
  }
}
