package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.internal.io.UnsupportedTypeException;

/**
 * A simple data set <i>extending</i> ObjectStore, used by ObjectStoreTest.testSubclass().
 */
public class IntegerStore extends ObjectStore<Integer> {

  public IntegerStore(String name) throws UnsupportedTypeException {
    super(name, Integer.class);
  }

  public void write(int key, Integer value) {
    super.write(Bytes.toBytes(key), value);
  }

  public Integer read(int key) {
    return super.read(Bytes.toBytes(key));
  }

}
