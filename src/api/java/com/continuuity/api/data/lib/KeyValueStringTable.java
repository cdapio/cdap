package com.continuuity.api.data.lib;

import com.continuuity.api.data.BatchCollectionRegistry;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.Write;
import com.continuuity.api.data.util.Bytes;

/**
 * A key-value table with Strings as keys and values.
 * <p>
 * Maps a string key to a string value.  Writes to existing keys will be
 * overwritten with new value.
 */
public class KeyValueStringTable extends KeyValueTable {

  /**
   * Constructs a key-value table with the specified name and using the
   * specified context.
   * @param tableName
   * @param fabric 
   * @param registry 
   */
  public KeyValueStringTable(String tableName, DataFabric fabric,
      BatchCollectionRegistry registry) {
    super(tableName, fabric, registry);
  }

  /**
   * Performs a synchronous read for the value of the specified key.
   * <p>
   * Returns null if no value found or empty string if key existed and
   * value is an empty value, otherwise, value is returned.
   * @param key key to read
   * @return value of key, null if key-value does not exist
   * @throws OperationException
   */
  public String read(String key) throws OperationException {
    byte [] value = read(Bytes.toBytes(key));
    if (value == null) return null;
    if (value.length == 0) return "";
    return Bytes.toString(value);
  }

  /**
   * Performs a synchronous write of the specified key-value.
   * @param key key to write
   * @param value value to write
   * @throws OperationException
   */
  public void performWrite(String key, String value)
      throws OperationException {
    this.fabric.execute(generateWrite(key, value));
  }

  /**
   * Outputs an asynchronous write of the specified key-value to be performed
   * as part of this process batch.
   * @param key key to write
   * @param value value to write
   */
  public void outputWrite(String key, String value) {
    getCollector().add(generateWrite(key, value));
  }

  /**
   * Generates a write operation for the specified key-value write operation.
   * @param key key to write
   * @param value value to write
   * @return write operation to store specified key-value
   */
  public Write generateWrite(String key, String value) {
    return generateWrite(Bytes.toBytes(key), Bytes.toBytes(value));
  }
}
