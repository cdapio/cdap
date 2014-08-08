/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.Schema;
import co.cask.cdap.internal.io.TypeRepresentation;
import com.continuuity.tephra.TransactionExecutor;
import com.continuuity.tephra.TransactionFailureException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Object store tests.
 */
public class MultiObjectStoreDatasetTest extends AbstractDatasetTest {

  private static final byte[] a = { 'a' };
  private static final byte[] DEFAULT_OBJECT_STORE_COLUMN = { 'c' };

  @Test
  public void testStringStore() throws Exception {
    create("strings", String.class);

    MultiObjectStoreDataset<String> stringStore = getInstance("strings");
    String string = "this is a string";
    stringStore.write(a, string);
    String result = stringStore.read(a);
    Assert.assertEquals(string, result);

    deleteInstance("strings");
  }

  @Test
  public void testPairStore() throws Exception {
    create("pairs", new TypeToken<ImmutablePair<Integer, String>>() {
    }.getType());

    MultiObjectStoreDataset<ImmutablePair<Integer, String>> pairStore = getInstance("pairs");
    ImmutablePair<Integer, String> pair = new ImmutablePair<Integer, String>(1, "second");
    pairStore.write(a, pair);
    ImmutablePair<Integer, String> result = pairStore.read(a);
    Assert.assertEquals(pair, result);

    deleteInstance("pairs");
  }

  @Test
  public void testMultiValueStore() throws Exception {
    create("multiString", String.class);

    MultiObjectStoreDataset<String> multiStringStore = getInstance("multiString");
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

    deleteInstance("multiString");
  }

  @Test
  public void testCustomStore() throws Exception {
    create("customs", Custom.class);

    MultiObjectStoreDataset<Custom> customStore = getInstance("customs");
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
    create("inners", new TypeToken<CustomWithInner.Inner<Integer>>() {
    }.getType());

    MultiObjectStoreDataset<CustomWithInner.Inner<Integer>> innerStore = getInstance("inners");
    CustomWithInner.Inner<Integer> inner = new CustomWithInner.Inner<Integer>(42, new Integer(99));
    innerStore.write(a, inner);
    CustomWithInner.Inner<Integer> result = innerStore.read(a);
    Assert.assertEquals(inner, result);

    deleteInstance("inners");
  }

  @Test
  public void testInstantiateWrongClass() throws Exception {
    create("pairs", new TypeToken<ImmutablePair<Integer, String>>() {
    }.getType());

    // note: due to type erasure, this succeeds
    final MultiObjectStoreDataset<Custom> store = getInstance("pairs");
    final MultiObjectStoreDataset<ImmutablePair<Integer, String>> pairStore = getInstance("pairs");

    TransactionExecutor storeTxnl = newTransactionExecutor(store);
    TransactionExecutor pairStoreTxnl = newTransactionExecutor(pairStore);

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

    pairStoreTxnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // write a correct object to the pair store
        ImmutablePair<Integer, String> pair = new ImmutablePair<Integer, String>(1, "second");
        pairStore.write(a, pair); // should succeed
      }
    });

    storeTxnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        try {
          Custom custom = store.read(a);
          Assert.fail("read should have failed with class cast exception");
        } catch (ClassCastException e) {
          // only this exception is expected (read will return a pair, but the assignment implicitly casts).
        }
      }
    });

    deleteInstance("pairs");
  }

  @Test
  public void testCustomClassLoader() throws Exception {
    // create a dummy class loader that records the name of the class it loaded
    final AtomicReference<String> lastClassLoaded = new AtomicReference<String>(null);
    ClassLoader loader = new ClassLoader() {
      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        lastClassLoaded.set(name);
        return super.loadClass(name);
      }
    };

    // create an instance that uses the dummy class loader
    createInstance("table", "table", DatasetProperties.EMPTY);

    Table table = getInstance("table");
    Type type = Custom.class;
    TypeRepresentation typeRep = new TypeRepresentation(type);
    Schema schema = new ReflectionSchemaGenerator().generate(type);

    MultiObjectStoreDataset<Custom> objectStore =
      new MultiObjectStoreDataset<Custom>("kv", table, typeRep, schema, loader);
    objectStore.write("dummy", new Custom(382, Lists.newArrayList("blah")));
    // verify the class name was recorded (the dummy class loader was used).
    Assert.assertEquals(Custom.class.getName(), lastClassLoaded.get());

    deleteInstance("table");
  }

  @Test
  public void testBatchReads() throws Exception {
    create("batch", String.class);

    final MultiObjectStoreDataset<String> t = getInstance("batch");
    TransactionExecutor txnl = newTransactionExecutor(t);
    final SortedSet<Long> keysWritten = Sets.newTreeSet();

    // start a transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // write 1000 random values to the table and remember them in a set
        Random rand = new Random(451);
        for (int i = 0; i < 1000; i++) {
          long keyLong = rand.nextLong();
          byte[] key = Bytes.toBytes(keyLong);
          t.write(key, Long.toString(keyLong));
          keysWritten.add(keyLong);
        }
      }
    });

    // start a sync transaction
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

    // start a sync transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // get specific number of splits for a subrange
        SortedSet<Long> keysToVerify = Sets.newTreeSet(keysWritten.subSet(0x10000000L, 0x40000000L));
        List<Split> splits = t.getSplits(5, Bytes.toBytes(0x10000000L), Bytes.toBytes(0x40000000L));
        Assert.assertTrue(splits.size() <= 5);
        // read each split and verify the keys
        verifySplits(t, splits, keysToVerify);
      }
    });

    deleteInstance("batch");
  }

  // helper to verify that the split readers for the given splits return exactly a set of keys
  private void verifySplits(MultiObjectStoreDataset<String> t, List<Split> splits, SortedSet<Long> keysToVerify)
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
    create("batchTestsMultiCol", String.class);

    final MultiObjectStoreDataset<String> t = getInstance("batchTestsMultiCol");
    TransactionExecutor txnl = newTransactionExecutor(t);
    final byte [] col1 = Bytes.toBytes("c1");
    final byte [] col2 = Bytes.toBytes("c2");
    final SortedSet<Integer> keysWritten = Sets.newTreeSet();

    // start a transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // write 1000 random values to the table and remember them in a set
        Random rand = new Random(451);
        for (int i = 0; i < 1000; i++) {
          int keyInt = rand.nextInt();
          byte[] key = Bytes.toBytes(keyInt);
          //write two columns
          t.write(key, col1, Long.toString(keyInt));
          t.write(key, col2, Long.toString(keyInt * 2));

          keysWritten.add(keyInt);
        }
      }
    });

    // start a sync transaction
    txnl.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
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
      }
    });

    deleteInstance("batchTestsMultiCol");
  }

  private void create(String instanceName, Type type) throws Exception {
    createInstance("multiObjectStore", instanceName, ObjectStores.objectStoreProperties(type, DatasetProperties.EMPTY));
  }
}
