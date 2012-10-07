package com.continuuity.api.data.lib;

import java.util.Map;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.Read;
import com.continuuity.api.data.StatusCode;
import com.continuuity.api.data.Write;

public class ObjectTable<T extends SimpleSerializable> extends DataLib {

  private static final byte [] COLUMN = new byte [] { 'd' };

  private final Class<T> storedObjectClass;

  public ObjectTable(DataFabric fabric, BatchCollectionRegistry registry,
      String tableName, Class<T> storedObjectClass) {
    super(tableName, fabric, registry);
    this.storedObjectClass = storedObjectClass;
  }

  public T readObject(byte [] id) throws OperationException {
    OperationResult<Map<byte[],byte[]>> result =
        this.getDataFabric().read(new Read(tableName, id, COLUMN));
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
    Write write = new Write(tableName, id, COLUMN, t.toBytes());
    this.getCollector().add(write);
  }

  public void writeObject(byte [] id, T t) throws OperationException {
    Write write = new Write(tableName, id, COLUMN, t.toBytes());
    this.getDataFabric().execute(write);
  }
}
