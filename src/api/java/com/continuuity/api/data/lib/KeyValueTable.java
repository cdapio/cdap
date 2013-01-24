package com.continuuity.api.data.lib;

import com.continuuity.api.data.*;
import com.continuuity.api.data.DataLib;

import java.util.Map;

/**
 * A key-value table.
 * <p>
 * Maps a binary key to a binary value.  Writes to existing keys will be
 * overwritten with new value.
 */
public class KeyValueTable extends DataLib {

  private static final byte [] COLUMN = new byte [] { 'c' };

  /**
   * Constructs a key-value table with the specified name and using the
   * specified context.
   * @param dataSetId Specifies the dataset Id.
   */
  public KeyValueTable(String dataSetId) {
    super(dataSetId, "KeyValueTable");
  }

  /**
   * Performs a synchronous read for the value of the specified key.
   * <p>
   * Returns null if no value found or zero-length byte array if key existed and
   * value is an empty value, otherwise, value is returned.
   * @param key key to read
   * @return value of key, null if key-value does not exist
   * @throws OperationException
   */
  public byte [] read(byte [] key) throws OperationException {
    OperationResult<Map<byte[],byte[]>> result =
      this.getDataFabric().read(
        new Read(getDataSetId(), key, COLUMN));
    if (result.isEmpty()) return null;
    return result.getValue().get(COLUMN);
  }

  /**
   * Performs a synchronous write of the specified key-value.
   * @param key key to write
   * @param value value to write
   * @throws OperationException
   */
  public void performWrite(byte [] key, byte [] value)
    throws OperationException {
    getDataFabric().execute(generateWrite(key, value));
  }

  /**
   * Outputs an asynchronous write of the specified key-value to be performed
   * as part of this process batch.
   * @param key key to write
   * @param value value to write
   */
  public void outputWrite(byte [] key, byte [] value) {
    getCollector().add(generateWrite(key, value));
  }

  /**
   * Generates a write operation for the specified key-value write operation.
   * @param key key to write
   * @param value value to write
   * @return write operation to store specified key-value
   */
  public Write generateWrite(byte [] key, byte [] value) {
    return new Write(getDataSetId(), key, COLUMN, value);
  }

}
