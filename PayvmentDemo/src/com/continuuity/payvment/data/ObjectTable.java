package com.continuuity.payvment.data;

import java.util.Map;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.Read;
import com.continuuity.api.data.Write;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.data.operation.StatusCode;

public class ObjectTable<T extends SimpleSerializable> extends DataLib {

  private static final byte [] COLUMN = new byte [] { 'c' };

  private final Class<T> storedObjectClass;

  public ObjectTable(FlowletContext context, String tableName,
      Class<T> storedObjectClass) {
    super(tableName, context);
    this.storedObjectClass = storedObjectClass;
  }

  public T readObject(byte [] id) throws OperationException {
    OperationResult<Map<byte[],byte[]>> result =
        this.context.getDataFabric().read(new Read(tableName, id, COLUMN));
    if (result.isEmpty()) return null;
    byte [] value = result.getValue().get(COLUMN);
    T t = null;
    try {
      t = storedObjectClass.newInstance();
    } catch (Exception e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage());
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
    this.context.getDataFabric().execute(write);
  }
}
