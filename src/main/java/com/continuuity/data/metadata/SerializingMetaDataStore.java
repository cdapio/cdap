package com.continuuity.data.metadata;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SerializingMetaDataStore implements MetaDataStore {

  private static final Logger Log =
      LoggerFactory.getLogger(SerializingMetaDataStore.class);

  private static final Charset charsetUTF8 = Charset.forName("UTF-8");
  private static final byte[] rowkey = string2Bytes("_metadata_");

  OperationExecutor opex;

  private static byte[] string2Bytes(String string) {
    return string.getBytes(charsetUTF8);
  }

  /*
  private String bytes2String(byte[] bytes) {
    return new String(bytes, charsetUTF8);
  }
  */

  /**
   * To avoid the overhead of creating new serializer for every call,
   * we keep a serializer for each thread in a thread local structure.
   */
  ThreadLocal<MetaDataSerializer> serializers =
      new ThreadLocal<MetaDataSerializer>();

  /**
   * Utility method to get or create the thread local serializer
   */
  MetaDataSerializer getSerializer() {
    if (this.serializers.get() == null) {
      this.serializers.set(new MetaDataSerializer());
    }
    return this.serializers.get();
  }

  byte[] makeColumnKey(String name, String type) {
    StringBuilder str = new StringBuilder();
    str.append(name);
    str.append('\0');
    str.append(type);
    return string2Bytes(str.toString());
  }

  byte[] makeColumnKey(MetaDataEntry meta) {
    return makeColumnKey(meta.getName(), meta.getType());
  }

  /*
  MetaDataEntry extractColumnKey(byte[] columnkey) throws MetaDataException {
    if (columnkey == null) {
      throw new MetaDataException(
          "byte representation of meta data row key is null.");
    }
    String str = bytes2String(columnkey);
    int idx = str.indexOf('\0');
    if (idx > 0 && idx < str.length() - 1) {
      String name = str.substring(0, idx);
      String type = str.substring(idx + 1);
      return new MetaDataEntry(name, type);
    }
    throw new MetaDataException(
        "invalid meta data column key \"" + str + "\". Valid values have the" +
            "form \"<name>\\0<type>\".");
  }
  */

  public SerializingMetaDataStore(OperationExecutor opex) {
    this.opex = opex;
  }

  @Override
  public void add(MetaDataEntry entry) throws MetaDataException {
    write(entry, false);
  }

  @Override
  public void update(MetaDataEntry entry) throws MetaDataException {
    write(entry, true);
  }

  private void write(MetaDataEntry entry, boolean isUpdate)
      throws MetaDataException {

    byte[] column = makeColumnKey(entry);
    byte[] bytes = getSerializer().serialize(entry);

    OperationResult<Map<byte[], byte[]>> result;
    try {
      Read read = new Read(rowkey, column);
      result = opex.execute(read);
    } catch (OperationException e) {
      String message =
          String.format("Error reading meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }

    if (isUpdate && result.isEmpty()) {
        throw new MetaDataException("Meta data entry does not exist.");
    }
    else if (!isUpdate && !result.isEmpty()) {
      throw new MetaDataException("Meta data entry already exists.");
    }

    // generate a write operation
    Write write = new Write(rowkey, column, bytes);

    try {
      opex.execute(write);
    } catch (OperationException e) {
      String message =
          String.format("Error writing meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }
  }

  @Override
  public MetaDataEntry get(String name, String type) throws MetaDataException {

    byte[] column = makeColumnKey(name, type);
    Read read = new Read(rowkey, column);

    try {
      OperationResult<Map<byte[], byte[]>> result;
      result = opex.execute(read);
      if (result.isEmpty()) return null;

      byte[] bytes = result.getValue().get(column);
      if (bytes == null) return null;

      return getSerializer().deserialize(bytes);

    } catch (OperationException e) {
      String message =
          String.format("Error reading meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }
  }

  @Override
  public List<MetaDataEntry> list(String type, Map<String, String> fields)
      throws MetaDataException {
    try {
      ReadColumnRange read = new ReadColumnRange(rowkey, null, null);
      OperationResult<Map<byte[],byte[]>> result = opex.execute(read);

      if (result.isEmpty())
        return Collections.emptyList();

      List<MetaDataEntry> entries = Lists.newArrayList();
      for (byte[] bytes : result.getValue().values()) {
        MetaDataEntry meta = getSerializer().deserialize(bytes);

        if (type != null && !type.equals(meta.getType()))
          continue;

        if (fields != null) {
          boolean match = true;
          for (Map.Entry<String,String> field : fields.entrySet()) {
            if (field.getValue() == null) {
              if (!meta.getTextFields().contains(field.getKey())) {
                match = false;
                break;
              }
            } else {
              if (!field.getValue().equals(meta.getTextField(field.getKey()))) {
                match = false;
                break;
              }
            }
          }
          if (!match) continue;
        }
        entries.add(meta);
      }

      return entries;

    } catch (OperationException e) {
      String message =
          String.format("Error reading meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }
  }

  @Override
  public List<MetaDataEntry> list(String type) throws MetaDataException {
    return list(type, null);
  }

  @Override
  public List<MetaDataEntry> list(Map<String, String> fields)
      throws MetaDataException {
    return list(null, fields);
  }
}
