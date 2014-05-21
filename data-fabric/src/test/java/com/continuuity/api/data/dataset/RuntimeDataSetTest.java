/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.data.dataset.DataSetTestBase;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Closeable;
import java.util.List;

/**
 * Test for getting DataSet at runtime, not through field injection.
 */
public class RuntimeDataSetTest extends DataSetTestBase {

  @BeforeClass
  public static void init() throws Exception {
    setupInstantiator(ImmutableList.of(
      new EmbeddedDataSet("MyDataSet"),
      new InstantiateFailureDataSet("Failure")
    ));

    newTransaction();
  }

  @Test
  public void testRuntimeDataSet() {
    EmbeddedDataSet dataSet = instantiator.getDataSet("MyDataSet");
    dataSet.put("key", "value");
    Assert.assertEquals("value.value", dataSet.get("key"));
  }

  @Test (expected = DataSetInstantiationException.class)
  public void testInstantiateFailure() {
    instantiator.getDataSet("Failure");
  }

  /**
   * A DataSet that simply delegate to another DataSet.
   */
  public static final class EmbeddedDataSet extends DataSet {
    private final ListDataSet listDataSet;

    public EmbeddedDataSet(String name) {
      super(name);
      this.listDataSet = new ListDataSet("embedded");
    }

    public void put(String key, String value) {
      listDataSet.put(key, value);
    }

    public String get(String key) {
      return listDataSet.get(key);
    }
  }

  /**
   * A DataSet that operates on a list of datasets.
   */
  public static final class ListDataSet extends DataSet {

    private List<? extends Closeable> dataSets;

    public ListDataSet(String name) {
      super(name);
    }

    @Override
    public DataSetSpecification configure() {
      try {
        return new DataSetSpecification.Builder(this)
          .datasets(new KeyValueTable("one"), new ObjectStore<String>("stringStore", String.class))
          .create();
      } catch (UnsupportedTypeException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void initialize(DataSetSpecification spec, DataSetContext context) {
      super.initialize(spec, context);
      dataSets = Lists.newArrayList(context.getDataSet("one"), context.getDataSet("stringStore"));
    }

    @SuppressWarnings("unchecked")
    public void put(String key, String value) {
      byte[] byteKey = Bytes.toBytes(key);
      ((KeyValueTable) dataSets.get(0)).write(byteKey, Bytes.toBytes(value));
      ((ObjectStore<String>) dataSets.get(1)).write(byteKey, value);
    }

    @SuppressWarnings("unchecked")
    public String get(String key) {
      byte[] byteKey = Bytes.toBytes(key);
      String value = Bytes.toString(((KeyValueTable) dataSets.get(0)).read(byteKey)) + '.' +
                     ((ObjectStore<String>) dataSets.get(1)).read(byteKey);
      return value;
    }
  }

  /**
   * DataSet that try to get a DataSet that is not defined.
   */
  public static final class InstantiateFailureDataSet extends DataSet {

    public InstantiateFailureDataSet(String name) {
      super(name);
    }

    @Override
    public void initialize(DataSetSpecification spec, DataSetContext context) {
      super.initialize(spec, context);
      context.getDataSet("invalid");
    }
  }
}
