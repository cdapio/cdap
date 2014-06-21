package com.continuuity.data2.dataset2.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.lib.IntegerStore;
import com.continuuity.api.dataset.lib.IntegerStoreModule;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.dataset.lib.ObjectStores;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data2.dataset2.AbstractDatasetTest;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.TypeRepresentation;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test for {@link com.continuuity.data2.dataset2.lib.table.ObjectStoreDataset}.
 */
public class ObjectStoreDatasetTest extends AbstractDatasetTest {

  private static final byte[] a = { 'a' };

  @Before
  public void setUp() throws Exception {
    super.setUp();
    addModule("core", new CoreDatasetsModule());
    addModule("integerStore", new IntegerStoreModule());
  }

  @After
  public void tearDown() throws Exception {
    deleteModule("integerStore");
    deleteModule("core");
    super.tearDown();
  }

  private void addIntegerStoreInstance(String instanceName) throws Exception {
    createInstance("integerStore", instanceName, DatasetProperties.EMPTY);
  }

  @Test
  public void testStringStore() throws Exception {
    createObjectStoreInstance("strings", String.class);
    
    ObjectStoreDataset<String> stringStore = getInstance("strings");
    String string = "this is a string";
    stringStore.write(a, string);
    String result = stringStore.read(a);
    Assert.assertEquals(string, result);

    deleteInstance("strings");
  }

  @Test
  public void testPairStore() throws Exception {
    createObjectStoreInstance("pairs", new TypeToken<ImmutablePair<Integer, String>>() { }.getType());

    ObjectStoreDataset<ImmutablePair<Integer, String>> pairStore = getInstance("pairs");
    ImmutablePair<Integer, String> pair = new ImmutablePair<Integer, String>(1, "second");
    pairStore.write(a, pair);
    ImmutablePair<Integer, String> result = pairStore.read(a);
    Assert.assertEquals(pair, result);

    deleteInstance("pairs");
  }

  @Test
  public void testCustomStore() throws Exception {
    createObjectStoreInstance("customs", new TypeToken<Custom>() { }.getType());

    ObjectStoreDataset<Custom> customStore = getInstance("customs");
    Custom custom = new Custom(42, Lists.newArrayList("one", "two"));
    customStore.write(a, custom);
    Custom result = customStore.read(a);
    Assert.assertEquals(custom, result);
    custom = new Custom(-1, null);
    customStore.write(a, custom);
    result = customStore.read(a);
    Assert.assertEquals(custom, result);

    deleteInstance("customs");
  }

  @Test
  public void testInnerStore() throws Exception {
    createObjectStoreInstance("inners", new TypeToken<CustomWithInner.Inner<Integer>>() { }.getType());

    ObjectStoreDataset<CustomWithInner.Inner<Integer>> innerStore = getInstance("inners");
    CustomWithInner.Inner<Integer> inner = new CustomWithInner.Inner<Integer>(42, new Integer(99));
    innerStore.write(a, inner);
    CustomWithInner.Inner<Integer> result = innerStore.read(a);
    Assert.assertEquals(inner, result);

    deleteInstance("inners");
  }

  @Test
  public void testInstantiateWrongClass() throws Exception {
    createObjectStoreInstance("pairs", new TypeToken<ImmutablePair<Integer, String>>() { }.getType());

    // note: due to type erasure, this succeeds
    final ObjectStoreDataset<Custom> store = getInstance("pairs");
    TransactionExecutor storeTxnl = newTransactionExecutor(store);
    // but now it must fail with incompatible type
    try {
      storeTxnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Custom custom = new Custom(42, Lists.newArrayList("one", "two"));
          store.write(a, custom);
        }
      });
      Assert.fail("write should have failed with incompatible type");
    } catch (TransactionFailureException e) {
      // expected
    }

    // write a correct object to the pair store
    final ObjectStoreDataset<ImmutablePair<Integer, String>> pairStore = getInstance("pairs");
    TransactionExecutor pairStoreTxnl = newTransactionExecutor(store);

    final ImmutablePair<Integer, String> pair = new ImmutablePair<Integer, String>(1, "second");
    pairStoreTxnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        pairStore.write(a, pair); // should succeed
      }
    });

    pairStoreTxnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        ImmutablePair<Integer, String> actualPair = pairStore.read(a);
        Assert.assertEquals(pair, actualPair);
      }
    });

    // now try to read that as a custom object, should fail with class cast
    try {
      storeTxnl.execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          Custom custom = store.read(a);
          Preconditions.checkNotNull(custom);
        }
      });
      Assert.fail("write should have failed with class cast exception");
    } catch (TransactionFailureException e) {
      // expected
    }

    deleteInstance("pairs");
  }

  @Test
  public void testWithCustomClassLoader() throws Exception {

    // create a dummy class loader that records the name of the class it loaded
    final AtomicReference<String> lastClassLoaded = new AtomicReference<String>(null);
    ClassLoader loader = new ClassLoader() {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        lastClassLoaded.set(name);
        return super.loadClass(name);
      }
    };

    createInstance("keyValueTable", "kv", DatasetProperties.EMPTY);

    KeyValueTable kvTable = getInstance("kv");
    Type type = Custom.class;
    TypeRepresentation typeRep = new TypeRepresentation(type);
    Schema schema = new ReflectionSchemaGenerator().generate(type);

    ObjectStoreDataset<Custom> objectStore = new ObjectStoreDataset<Custom>("kv", kvTable, typeRep, schema, loader);
    objectStore.write("dummy", new Custom(382, Lists.newArrayList("blah")));
    // verify the class name was recorded (the dummy class loader was used).
    Assert.assertEquals(Custom.class.getName(), lastClassLoaded.get());

    deleteInstance("kv");
  }

  @Test
  public void testBatchCustomList() throws Exception {
    createObjectStoreInstance("customlist", new TypeToken<List<Custom>>() { }.getType());

    final ObjectStoreDataset<List<Custom>> customStore = getInstance("customlist");
    TransactionExecutor txnl = newTransactionExecutor(customStore);

    final SortedSet<Long> keysWritten = Sets.newTreeSet();

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        List<Custom> customList1 = Arrays.asList(new Custom(1, Lists.newArrayList("one", "ONE")),
                                                 new Custom(2, Lists.newArrayList("two", "TWO")));
        Random rand = new Random(100);
        long key1 = rand.nextLong();
        keysWritten.add(key1);

        customStore.write(Bytes.toBytes(key1), customList1);

        List<Custom> customList2 = Arrays.asList(new Custom(3, Lists.newArrayList("three", "THREE")),
                                                 new Custom(4, Lists.newArrayList("four", "FOUR")));
        long key2 = rand.nextLong();
        keysWritten.add(key2);

        customStore.write(Bytes.toBytes(key2), customList2);
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // get the splits for the table
        List<Split> splits = customStore.getSplits();

        for (Split split : splits) {
          SplitReader<byte[], List<Custom>> reader = customStore.createSplitReader(split);
          reader.initialize(split);
          while (reader.nextKeyValue()) {
            byte[] key = reader.getCurrentKey();
            Assert.assertTrue(keysWritten.remove(Bytes.toLong(key)));
          }
        }
        // verify all keys have been read
        if (!keysWritten.isEmpty()) {
          System.out.println("Remaining [" + keysWritten.size() + "]: " + keysWritten);
        }
        Assert.assertTrue(keysWritten.isEmpty());
      }
    });

    deleteInstance("customlist");
  }

  @Test
  public void testBatchReads() throws Exception {
    createObjectStoreInstance("batch", String.class);

    final ObjectStoreDataset<String> t = getInstance("batch");
    TransactionExecutor txnl = newTransactionExecutor(t);

    final SortedSet<Long> keysWritten = Sets.newTreeSet();

    // write 1000 random values to the table and remember them in a set
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Random rand = new Random(451);
        for (int i = 0; i < 1000; i++) {
          long keyLong = rand.nextLong();
          byte[] key = Bytes.toBytes(keyLong);
          t.write(key, Long.toString(keyLong));
          keysWritten.add(keyLong);
        }
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // get the splits for the table
        List<Split> splits = t.getSplits();
        // read each split and verify the keys
        SortedSet<Long> keysToVerify = Sets.newTreeSet(keysWritten);
        verifySplits(t, splits, keysToVerify);
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // get specific number of splits for a subrange
        TreeSet<Long> keysToVerify = Sets.newTreeSet(keysWritten.subSet(0x10000000L, 0x40000000L));
        List<Split> splits = t.getSplits(5, Bytes.toBytes(0x10000000L), Bytes.toBytes(0x40000000L));
        Assert.assertTrue(splits.size() <= 5);
        // read each split and verify the keys
        verifySplits(t, splits, keysToVerify);
      }
    });

    deleteInstance("batch");
  }

  // helper to verify that the split readers for the given splits return exactly a set of keys
  private void verifySplits(ObjectStoreDataset<String> t, List<Split> splits, SortedSet<Long> keysToVerify)
    throws InterruptedException {
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
  public void testSubclass() throws Exception {
    addIntegerStoreInstance("ints");

    IntegerStore ints = getInstance("ints");
    ints.write(42, 101);
    Assert.assertEquals((Integer) 101, ints.read(42));

    deleteInstance("ints");
  }

  private void createObjectStoreInstance(String instanceName, Type type) throws Exception {
    createInstance("objectStore", instanceName, ObjectStores.objectStoreProperties(type, DatasetProperties.EMPTY));
  }
}
