package com.continuuity.data.metadata;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.google.common.collect.Lists;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;

public class ColumnarMetaDataStore implements MetaDataStore {

  private static final Logger Log =
      LoggerFactory.getLogger(ColumnarMetaDataStore.class);

  private static final Charset charsetUTF8 = Charset.forName("UTF-8");

  private static final String prefix = "_metadata_";
  private static final byte[] typeColumn = name2Bytes("type", false);
  private static final byte[] emptyValue = new byte[0];

  OperationExecutor opex;

  private static final char textPrefix = '"';
  private static final char binaryPrefix = '_';

  private static byte[] string2Bytes(String string) {
    return string.getBytes(charsetUTF8);
  }

  private static byte[] name2Bytes(String name, boolean isBinary) {
    char prefix = isBinary ? binaryPrefix : textPrefix;
    return string2Bytes(prefix + name);
  }

  private static boolean isBinaryField(byte[] bytes) throws MetaDataException {
    if (bytes == null) {
      throw new MetaDataException(
          "byte representation of meta data name is null.");
    }
    if (bytes.length < 1) {
      throw new MetaDataException(
          "byte representation of meta data name is empty.");
    }
    return binaryPrefix == bytes[0];
  }

  private String bytes2String(byte[] bytes) {
    return new String(bytes, charsetUTF8);
  }

  private String bytes2Name(byte[] bytes) {
    return new String(Arrays.copyOfRange(bytes, 1, bytes.length), charsetUTF8);
  }

  class Row {
    byte[] rowkey;
    byte[][] columns;
    byte[][] values;
  }

  byte[] makeRowKey(String name, String type) {
    StringBuilder str = new StringBuilder(prefix);
    str.append(name);
    str.append('_');
    str.append(type);
    return string2Bytes(str.toString());
  }

  byte[] makeRowKey(MetaDataEntry meta) {
    return makeRowKey(meta.getName(), meta.getType());
  }

  /*
  MetaDataEntry extractRowKey(byte[] rowkey) throws MetaDataException {
    if (rowkey == null) {
      throw new MetaDataException(
          "byte representation of meta data row key is null.");
    }
    String str = bytes2String(rowkey);
    if (str.startsWith(prefix)) {
      int idx = str.indexOf('_', prefix.length());
      if (idx > prefix.length() && idx < str.length() - 1) {
        String name = str.substring(prefix.length(), idx);
        String type = str.substring(idx + 1);
        return new MetaDataEntry(name, type);
      }
    }
    throw new MetaDataException(
        "invalid meta data row key \"" + str + "\". Valid values have the" +
            "form \"_metadata_<name>_<type>\".");
  }
  */

  Row meta2Row(MetaDataEntry meta) {
    Row row = new Row();
    row.rowkey = makeRowKey(meta);

    List<byte[]> columns = Lists.newArrayList();
    List<byte[]> values = Lists.newArrayList();

    columns.add(typeColumn);
    values.add(string2Bytes(meta.getType()));

    for (String key : meta.getTextFields()) {
      columns.add(name2Bytes(key, false));
      values.add(string2Bytes(meta.getTextField(key)));
    }
    for (String key : meta.getBinaryFields()) {
      columns.add(name2Bytes(key, true));
      values.add(meta.getBinaryField(key));
    }

    row.columns = columns.toArray(new byte[columns.size()][]);
    row.values = values.toArray(new byte[values.size()][]);

    return row;
  }

  MetaDataEntry row2Meta(String name, String type, Map<byte[], byte[]> row)
      throws MetaDataException {

    MetaDataEntry meta = new MetaDataEntry(name, type);
    for (Map.Entry<byte[], byte[]> entry : row.entrySet()) {
      byte[] column = entry.getKey();
      byte[] value = entry.getValue() == null ? emptyValue : entry.getValue();
      if (isBinaryField(column)) {
        meta.addField(bytes2Name(column), value);
      } else {
        if (Arrays.equals(typeColumn, column)) {
          if (!type.equals(bytes2String(value))) {
            throw new MetaDataException(
                "Value of type column is \"" + bytes2String(value) +
                    "\" but expected type is \"" + type + "\".");
          }
        } else {
          meta.addField(bytes2Name(column), bytes2String(value));
        }
      }
    }
    return meta;
  }

  public ColumnarMetaDataStore(OperationExecutor opex) {
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

    Row row = meta2Row(entry);

    OperationResult<Map<byte[], byte[]>> result;
    try {
      ReadColumnRange read = new ReadColumnRange(row.rowkey, null, null);
      result = opex.execute(read);
    } catch (OperationException e) {
      String message =
          String.format("Error reading meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }

    List<WriteOperation> ops = new ArrayList<WriteOperation>();
    if (isUpdate) {
      if (result.isEmpty()) {
        throw new MetaDataException("Meta data entry does not exist.");
      } else {
        // generate a delete for existing columns not present in the new meta
        for (byte[] column : row.columns)
          result.getValue().remove(column);
        Set<byte[]> columnsToDelete = result.getValue().keySet();
        ops.add(new Delete(row.rowkey,
            columnsToDelete.toArray(new byte[columnsToDelete.size()][])));
      }
    }
    else if (!result.isEmpty()) {
      throw new MetaDataException("Meta data entry already exists.");
    }

    // generate a write operation
    ops.add(new Write(row.rowkey, row.columns, row.values));

    try {
      opex.execute(ops);
    } catch (OperationException e) {
      String message =
          String.format("Error writing meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }
  }

  @Override
  public MetaDataEntry get(String name, String type) throws MetaDataException {

    byte[] rowkey = makeRowKey(name, type);
    ReadColumnRange read = new ReadColumnRange(rowkey, null, null);

    OperationResult<Map<byte[], byte[]>> result;
    try {
      result = opex.execute(read);
    } catch (OperationException e) {
      String message =
          String.format("Error reading meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }

    if (result.isEmpty()) return null;
    return row2Meta(name, type, result.getValue());
  }

  @Override
  public void delete(String name, String type) throws MetaDataException {

    byte[] rowkey = makeRowKey(name, type);
    OperationResult<Map<byte[], byte[]>> result;
    try {
      ReadColumnRange read = new ReadColumnRange(rowkey, null, null);
      result = opex.execute(read);
    } catch (OperationException e) {
      String message =
          String.format("Error reading meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }

    if (result.isEmpty()) return; // nothing there to delete

    Set<byte[]> columns = result.getValue().keySet();
    Delete delete = new Delete(rowkey,
        columns.toArray(new byte[columns.size()][]));

    try {
      opex.execute(delete);
    } catch (OperationException e) {
      String message =
          String.format("Error deleting meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }
  }

  @Override
  public List<MetaDataEntry> list(String type) {
    throw new NotImplementedException();
  }

  @Override
  public List<MetaDataEntry> list(Map<String, String> fields) {
    throw new NotImplementedException();
  }

  @Override
  public List<MetaDataEntry> list(String type, Map<String, String> fields) {
    throw new NotImplementedException();
  }

  @Override
  public void clear() throws MetaDataException {
    throw new NotImplementedException();
  }
}
