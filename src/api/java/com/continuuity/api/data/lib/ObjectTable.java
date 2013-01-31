package com.continuuity.api.data.lib;

import com.continuuity.api.data.*;
import com.continuuity.api.data.DataLib;

import java.util.Map;

/**
 * Generic ObjectTable
 * @param <T> the template should extend SimpleSerializable
 */
public class ObjectTable<T extends SimpleSerializable> extends DataLib {
  private static final byte [] COLUMN = new byte [] { 'd' };
  private final Class<T> storedObjectClass;

  /**
   * Constructor
   * @param dataSetId DataSetID
   * @param storedObjectClass class name of the data type to be stored and retrieved using ObjectTable.
   */
  public ObjectTable(String dataSetId, Class<T> storedObjectClass) {
    super(dataSetId, "ObjectTable");
    this.storedObjectClass = storedObjectClass;
  }

  /**
   * Read Object
   * @param id  id of the object to be read
   * @return instance of the data that is stored
   * @throws OperationException
   */
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

  /**
   * Update object
   * @param id id of the object to be updated
   * @param t instance of the data that is updated
   */
  public void updateObject(byte [] id, T t) {
    Write write = new Write(getDataSetId(), id, COLUMN, t.toBytes());
    this.getCollector().add(write);
  }

  /**
   * Write Object
   * @param id   id of the object to be written
   * @param t  instance of the data to be written
   * @throws OperationException
   */
  public void writeObject(byte [] id, T t) throws OperationException {
    Write write = new Write(getDataSetId(), id, COLUMN, t.toBytes());
    this.getDataFabric().execute(write);
  }
}
