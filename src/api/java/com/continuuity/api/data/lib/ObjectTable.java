package com.continuuity.api.data.lib;

import com.continuuity.api.data.*;
import com.continuuity.api.data.DataLib;

import java.util.Map;

public class ObjectTable<T extends SimpleSerializable> extends DataLib {
  private static final byte [] COLUMN = new byte [] { 'd' };
  private final Class<T> storedObjectClass;

  public ObjectTable(String dataSetId, Class<T> storedObjectClass) {
    super(dataSetId, "ObjectTable");
    this.storedObjectClass = storedObjectClass;
  }

  public T readObject(byte [] id) throws OperationException {
    OperationResult<Map<byte[],byte[]>> result =
      this.getDataFabric().read(new Read(getDataSetId(), id, COLUMN));
    if (result.isEmpty()) return null;
    byte [] value = result.getValue().get(COLUMN);
    T t = null;
    try {
      t = storedObjectClass.newInstance();
    } catch (Exception e) {
      throw new OperationException(StatusCode.KEY_NOT_FOUND, e.getMessage());
    }
    t.fromBytes(value);
    return t;
  }

  public void updateObject(byte [] id, T t) {
    Write write = new Write(getDataSetId(), id, COLUMN, t.toBytes());
    this.getCollector().add(write);
  }

  public void writeObject(byte [] id, T t) throws OperationException {
    Write write = new Write(getDataSetId(), id, COLUMN, t.toBytes());
    this.getDataFabric().execute(write);
  }
}
