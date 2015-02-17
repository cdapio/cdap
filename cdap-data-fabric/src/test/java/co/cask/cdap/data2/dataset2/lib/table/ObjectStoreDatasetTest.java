/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.IntegerStore;
import co.cask.cdap.api.dataset.lib.IntegerStoreModule;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.TypeRepresentation;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test for {@link co.cask.cdap.data2.dataset2.lib.table.ObjectStoreDataset}.
 */
public class ObjectStoreDatasetTest extends AbstractDatasetTest {

  private static final byte[] a = { 'a' };

  private static final Id.DatasetModule integerStore = Id.DatasetModule.from(NAMESPACE_ID, "integerStore");

  @BeforeClass
  public static void beforeClass() throws Exception {
    addModule(integerStore, new IntegerStoreModule());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    deleteModule(integerStore);
  }

  private void addIntegerStoreInstance(Id.DatasetInstance datasetInstanceId) throws Exception {
    createInstance("integerStore", datasetInstanceId, DatasetProperties.EMPTY);
  }

  @Test
  public void testStringStore() throws Exception {
    Id.DatasetInstance strings = Id.DatasetInstance.from(NAMESPACE_ID, "strings");
    createObjectStoreInstance(strings, String.class);
    
    ObjectStoreDataset<String> stringStore = getInstance(strings);
    String string = "this is a string";
    stringStore.write(a, string);
    String result = stringStore.read(a);
    Assert.assertEquals(string, result);

    deleteAndVerify(stringStore, a);

    deleteInstance(strings);
  }

  @Test
  public void testPairStore() throws Exception {
    Id.DatasetInstance pairs = Id.DatasetInstance.from(NAMESPACE_ID, "pairs");
    createObjectStoreInstance(pairs, new TypeToken<ImmutablePair<Integer, String>>() { }.getType());

    ObjectStoreDataset<ImmutablePair<Integer, String>> pairStore = getInstance(pairs);
    ImmutablePair<Integer, String> pair = new ImmutablePair<Integer, String>(1, "second");
    pairStore.write(a, pair);
    ImmutablePair<Integer, String> result = pairStore.read(a);
    Assert.assertEquals(pair, result);

    deleteAndVerify(pairStore, a);

    deleteInstance(pairs);
  }

  @Test
  public void testCustomStore() throws Exception {
    Id.DatasetInstance customs = Id.DatasetInstance.from(NAMESPACE_ID, "customs");
    createObjectStoreInstance(customs, new TypeToken<Custom>() { }.getType());

    ObjectStoreDataset<Custom> customStore = getInstance(customs);
    Custom custom = new Custom(42, Lists.newArrayList("one", "two"));
    customStore.write(a, custom);
    Custom result = customStore.read(a);
    Assert.assertEquals(custom, result);
    custom = new Custom(-1, null);
    customStore.write(a, custom);
    result = customStore.read(a);
    Assert.assertEquals(custom, result);

    deleteAndVerify(customStore, a);

    deleteInstance(customs);
  }

  @Test
  public void testInnerStore() throws Exception {
    Id.DatasetInstance inners = Id.DatasetInstance.from(NAMESPACE_ID, "inners");
    createObjectStoreInstance(inners, new TypeToken<CustomWithInner.Inner<Integer>>() { }.getType());

    ObjectStoreDataset<CustomWithInner.Inner<Integer>> innerStore = getInstance(inners);
    CustomWithInner.Inner<Integer> inner = new CustomWithInner.Inner<Integer>(42, new Integer(99));
    innerStore.write(a, inner);
    CustomWithInner.Inner<Integer> result = innerStore.read(a);
    Assert.assertEquals(inner, result);

    deleteAndVerify(innerStore, a);

    deleteInstance(inners);
  }

  @Test
  public void testInstantiateWrongClass() throws Exception {
    Id.DatasetInstance pairs = Id.DatasetInstance.from(NAMESPACE_ID, "pairs");
    createObjectStoreInstance(pairs, new TypeToken<ImmutablePair<Integer, String>>() { }.getType());

    // note: due to type erasure, this succeeds
    final ObjectStoreDataset<Custom> store = getInstance(pairs);
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
    final ObjectStoreDataset<ImmutablePair<Integer, String>> pairStore = getInstance(pairs);
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

    deleteAndVerify(pairStore, a);

    deleteInstance(pairs);
  }

  @Test
  public void testWithCustomClassLoader() throws Exception {
    Id.DatasetInstance kv = Id.DatasetInstance.from(NAMESPACE_ID, "kv");
    // create a dummy class loader that records the name of the class it loaded
    final AtomicReference<String> lastClassLoaded = new AtomicReference<String>(null);
    ClassLoader loader = new ClassLoader() {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        lastClassLoaded.set(name);
        return super.loadClass(name);
      }
    };

    createInstance("keyValueTable", kv, DatasetProperties.EMPTY);

    KeyValueTable kvTable = getInstance(kv);
    Type type = Custom.class;
    TypeRepresentation typeRep = new TypeRepresentation(type);
    Schema schema = new ReflectionSchemaGenerator().generate(type);

    ObjectStoreDataset<Custom> objectStore = new ObjectStoreDataset<Custom>("kv", kvTable, typeRep, schema, loader);
    objectStore.write("dummy", new Custom(382, Lists.newArrayList("blah")));
    // verify the class name was recorded (the dummy class loader was used).
    Assert.assertEquals(Custom.class.getName(), lastClassLoaded.get());

    deleteAndVerify(objectStore, Bytes.toBytes("dummy"));

    deleteInstance(kv);
  }

  @Test
  public void testBatchCustomList() throws Exception {
    Id.DatasetInstance customlist = Id.DatasetInstance.from(NAMESPACE_ID, "customlist");
    createObjectStoreInstance(customlist, new TypeToken<List<Custom>>() { }.getType());

    final ObjectStoreDataset<List<Custom>> customStore = getInstance(customlist);
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

    final SortedSet<Long> keysWrittenCopy = ImmutableSortedSet.copyOf(keysWritten);

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

    deleteAndVerifyInBatch(customStore, txnl, keysWrittenCopy);

    deleteInstance(customlist);
  }

  @Test
  public void testBatchReads() throws Exception {
    Id.DatasetInstance batch = Id.DatasetInstance.from(NAMESPACE_ID, "batch");
    createObjectStoreInstance(batch, String.class);

    final ObjectStoreDataset<String> t = getInstance(batch);
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

    deleteAndVerifyInBatch(t, txnl, keysWritten);

    deleteInstance(batch);
  }

  @Test
  public void testScanObjectStore() throws Exception {
    Id.DatasetInstance scan = Id.DatasetInstance.from(NAMESPACE_ID, "scan");
    createObjectStoreInstance(scan, String.class);

    final ObjectStoreDataset<String> t = getInstance(scan);
    TransactionExecutor txnl = newTransactionExecutor(t);

    // write 10 values
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (int i = 0; i < 10; i++) {
          byte[] key = Bytes.toBytes(i);
          t.write(key, String.valueOf(i));
        }
      }
    });

    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Iterator<KeyValue<byte[], String>> objectsIterator = t.scan(Bytes.toBytes(0), Bytes.toBytes(10));
        int sum = 0;
        while (objectsIterator.hasNext()) {
          sum += Integer.parseInt(objectsIterator.next().getValue());
        }
        //checking the sum equals sum of values from (0..9) which are the rows written and scanned for.
        Assert.assertEquals(45, sum);
      }
    });

    // start a transaction, scan part of them elements using scanner, close the scanner,
    // then call next() on scanner, it should fail
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        CloseableIterator<KeyValue<byte[], String>> objectsIterator = t.scan(Bytes.toBytes(0), Bytes.toBytes(10));
        int rowCount = 0;
        while (objectsIterator.hasNext() && (rowCount < 5)) {
          rowCount++;
        }
        objectsIterator.close();
        try {
          objectsIterator.next();
          Assert.fail("Reading after closing Scanner returned result.");
        } catch (NoSuchElementException e) {
        }
      }
    });


    deleteInstance(scan);
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
    Id.DatasetInstance intsInstance = Id.DatasetInstance.from(NAMESPACE_ID, "ints");
    addIntegerStoreInstance(intsInstance);

    IntegerStore ints = getInstance(intsInstance);
    ints.write(42, 101);
    Assert.assertEquals((Integer) 101, ints.read(42));

    // test delete
    ints.delete(42);
    Assert.assertNull(ints.read(42));

    deleteInstance(intsInstance);
  }

  private void createObjectStoreInstance(Id.DatasetInstance datasetInstanceId, Type type) throws Exception {
    createInstance("objectStore", datasetInstanceId, ObjectStores.objectStoreProperties(type, DatasetProperties.EMPTY));
  }

  private void deleteAndVerify(ObjectStore store, byte[] key) {
    store.delete(key);
    Assert.assertNull(store.read(key));
  }

  private void deleteAndVerifyInBatch(final ObjectStoreDataset t, TransactionExecutor txnl,
                                      final SortedSet<Long> keysWritten) throws TransactionFailureException,
                                      InterruptedException {
    // delete all the keys written earlier
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Long curKey : keysWritten) {
          t.delete(Bytes.toBytes(curKey));
        }
      }
    });

    // verify that all the keys are deleted
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        for (Long curKey : keysWritten) {
          Assert.assertNull(t.read(Bytes.toBytes(curKey)));
        }
      }
    });
  }
}
