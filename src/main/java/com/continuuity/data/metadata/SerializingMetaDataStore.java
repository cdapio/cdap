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
import java.util.Set;

public class SerializingMetaDataStore implements MetaDataStore {

  private static final Logger Log =
      LoggerFactory.getLogger(SerializingMetaDataStore.class);

  OperationExecutor opex;

  private static final Charset charsetUTF8 = Charset.forName("UTF-8");

  private static String rowkeyPrefix = "_metadata_";

  private static byte[] string2Bytes(String string) {
    return string.getBytes(charsetUTF8);
  }
  private static String bytes2String(byte[] bytes) {
    return new String(bytes, charsetUTF8);
  }

  private static byte[] makeRowKey(String account) {
    StringBuilder str = new StringBuilder();
    str.append(rowkeyPrefix);
    str.append('\0');
    str.append(account);
    return string2Bytes(str.toString());
  }

  private static byte[] makeRowKey(MetaDataEntry meta) {
    return makeRowKey(meta.getAccount());
  }

  private static byte[] makeColumnKey(String app, String type, String id) {
    StringBuilder str = new StringBuilder();
    str.append(type);
    str.append('\0');
    if (app != null) str.append(app);
    str.append('\0');
    str.append(id);
    return string2Bytes(str.toString());
  }

  byte[] makeColumnKey(MetaDataEntry meta) {
    return makeColumnKey(meta.getApplication(), meta.getType(), meta.getId());
  }

  private static byte[] startColumnKey(String app, String type) {
    StringBuilder str = new StringBuilder();
    str.append(type);
    if (app != null) {
      str.append('\0');
      str.append(app);
    }
    str.append('\0');
    return string2Bytes(str.toString());
  }

  private static byte[] stopColumnKey(String app, String type) {
    StringBuilder str = new StringBuilder();
    str.append(type);
    if (app != null) {
      str.append('\0');
      str.append(app);
    }
    str.append('\1');
    return string2Bytes(str.toString());
  }

  private static String extractApplication(byte[] columnKey) {
    String str = bytes2String(columnKey);
    int pos = str.indexOf('\0');
    if (pos < 0) return null;
    int pos1 = str.indexOf('\0', pos + 1);
    if (pos1 < 0) return null;
    if (pos1 == pos + 1) return null;
    return str.substring(pos + 1, pos1);
  }

  /*
  private static final byte[] rowkeyPrefix = string2Bytes("_metadata_");
  private static final byte[] mtBytes = new byte[0];

  private static byte[] makeRowkey(String account) {
    byte[] accountBytes = string2Bytes(account);
    byte[] bytes = new byte[rowkeyPrefix.length + accountBytes.length];
    System.arraycopy(rowkeyPrefix, 0, bytes, 0, rowkeyPrefix.length);
    System.arraycopy(accountBytes, 0, bytes, rowkeyPrefix.length,
        accountBytes.length);
    return bytes;
  }

  private static byte[] makeColumnKey(String type, String app, String id) {
    byte[] typeBytes = string2Bytes(type);
    byte[] appBytes = app == null ? mtBytes : string2Bytes(app);
    byte[] idBytes = string2Bytes(id);
    byte[] bytes = new byte[typeBytes.length + appBytes.length
        + idBytes.length + 2];
    System.arraycopy(typeBytes, 0, bytes, 0, typeBytes.length);
    System.arraycopy(appBytes, 0, bytes, typeBytes.length + 1, appBytes.length);
    System.arraycopy(idBytes, 0, bytes,
        typeBytes.hashCode() + appBytes.length + 2, idBytes.length);
    return bytes;
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

    if (entry == null)
      throw new IllegalArgumentException("entry cannot be null");

    byte[] rowkey = makeRowKey(entry);
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
  public MetaDataEntry get(String account, String application,
                           String type, String id) throws MetaDataException {

    if (account == null)
      throw new IllegalArgumentException("account cannot be null");
    if (account.isEmpty())
      throw new IllegalArgumentException("account cannot be empty");
    if (id == null)
      throw new IllegalArgumentException("id cannot be null");
    if (id.isEmpty())
      throw new IllegalArgumentException("id cannot be empty");
    if (application != null && application.isEmpty())
      throw new IllegalArgumentException("application cannot be empty");
    if (type == null)
      throw new IllegalArgumentException("type cannot be null");
    if (type.isEmpty())
      throw new IllegalArgumentException("type cannot be empty");

    byte[] rowkey = makeRowKey(account);
    byte[] column = makeColumnKey(application, type, id);

    try {
      Read read = new Read(rowkey, column);
      OperationResult<Map<byte[], byte[]>> result = opex.execute(read);

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
  public void delete(String account, String application,
                     String type, String id) throws MetaDataException {

    if (account == null)
      throw new IllegalArgumentException("account cannot be null");
    if (account.isEmpty())
      throw new IllegalArgumentException("account cannot be empty");
    if (id == null)
      throw new IllegalArgumentException("id cannot be null");
    if (id.isEmpty())
      throw new IllegalArgumentException("id cannot be empty");
    if (application != null && application.isEmpty())
      throw new IllegalArgumentException("application cannot be empty");
    if (type == null)
      throw new IllegalArgumentException("type cannot be null");
    if (type.isEmpty())
      throw new IllegalArgumentException("type cannot be empty");

    byte[] rowkey = makeRowKey(account);
    byte[] column = makeColumnKey(application, type, id);

    try {
      opex.execute(new Delete(rowkey, column));

    } catch (OperationException e) {
      String message =
          String.format("Error deleting meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }
  }

  @Override
  public List<MetaDataEntry> list(String account, String application,
                                  String type, Map<String, String> fields)
      throws MetaDataException {
    try {
      if (account == null)
        throw new IllegalArgumentException("account cannot be null");
      if (account.isEmpty())
        throw new IllegalArgumentException("account cannot be empty");
      if (application != null && application.isEmpty())
        throw new IllegalArgumentException("application cannot be empty");
      if (type == null)
        throw new IllegalArgumentException("type cannot be null");
      if (type.isEmpty())
        throw new IllegalArgumentException("type cannot be empty");

      byte[] rowkey = makeRowKey(account);
      byte[] start = startColumnKey(application, type);
      byte[] stop = stopColumnKey(application, type);
      ReadColumnRange read = new ReadColumnRange(rowkey, start, stop);
      OperationResult<Map<byte[],byte[]>> result = opex.execute(read);

      if (result.isEmpty())
        return Collections.emptyList();

      List<MetaDataEntry> entries = Lists.newArrayList();
      for (byte[] bytes : result.getValue().values()) {
        MetaDataEntry meta = getSerializer().deserialize(bytes);

        if (!type.equals(meta.getType()))
          continue;
        if (application != null && !application.equals(meta.getApplication()))
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
  public void clear(String account, String application)
      throws MetaDataException {

    if (account == null)
      throw new IllegalArgumentException("account cannot be null");
    if (account.isEmpty())
      throw new IllegalArgumentException("account cannot be empty");
    if (application != null && application.isEmpty())
      throw new IllegalArgumentException("application cannot be empty");

    byte[] rowkey = makeRowKey(account);
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

    byte[][] columns;
    Set<byte[]> colset = result.getValue().keySet();
    if (application == null) {
      columns = colset.toArray(new byte[colset.size()][]);
    } else {
      List<byte[]> cols = Lists.newArrayList();
      for (byte[] colkey : colset) {
        if (application.equals(extractApplication(colkey)))
          cols.add(colkey);
      }
      columns = cols.toArray(new byte[cols.size()][]);
    }

    Delete delete = new Delete(rowkey, columns);
    try {
      opex.execute(delete);
    } catch (OperationException e) {
      String message =
          String.format("Error clearing meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }
  }
}
