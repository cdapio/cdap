package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.internal.io.UnsupportedTypeException;

/**
 * A simple data set <i>extending</i> ObjectStore, used by ObjectStoreTest.testSubclass().
 */
public class IntegerStore extends ObjectStore<Integer> {

  public IntegerStore(String name) throws UnsupportedTypeException {
    super(name, Integer.class);
  }

  @SuppressWarnings("unused")
  public IntegerStore(DataSetSpecification spec) {
    super(spec);
  }

  public void write(int key, Integer value) throws OperationException {
    super.write(Bytes.toBytes(key), value);
  }

  public Integer read(int key) throws OperationException {
    return super.read(Bytes.toBytes(key));
  }

}
