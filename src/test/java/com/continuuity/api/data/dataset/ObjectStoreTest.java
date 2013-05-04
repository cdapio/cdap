package com.continuuity.api.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.StatusCode;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.internal.api.io.UnsupportedTypeException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.SortedSet;

public class ObjectStoreTest extends DataSetTestBase {

  static final byte[] a = { 'a' };

  @BeforeClass
  public static void configure() throws Exception {
    DataSet stringStore = new ObjectStore<String>("strings", String.class);
    DataSet pairStore = new ObjectStore<ImmutablePair<Integer, String>>(
      "pairs", new TypeToken<ImmutablePair<Integer, String>>(){}.getType());
    DataSet customStore = new ObjectStore<Custom>("customs", Custom.class);
    DataSet innerStore = new ObjectStore<CustomWithInner.Inner<Integer>>(
      "inners", new TypeToken<CustomWithInner.Inner<Integer>>(){}.getType());
    setupInstantiator(Lists.newArrayList(stringStore, pairStore, customStore, innerStore));
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
    if (this == o) return true;
    if (o == null || o.getClass() != this.getClass()) return false;
    if (this.i != ((Custom)o).i) return false;
    if (this.sl == null) return ((Custom)o).sl == null;
    return this.sl.equals(((Custom)o).sl);
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
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Inner inner = (Inner) o;
      if (set != null ? !set.equals(inner.set) : inner.set != null) return false;
      if (x != null ? !x.equals(inner.x) : inner.x != null) return false;
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
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CustomWithInner that = (CustomWithInner) o;
    if (a != null ? !a.equals(that.a) : that.a != null) return false;
    return true;
  }

  @Override
  public int hashCode() {
    return a != null ? a.hashCode() : 0;
  }
}
