package com.continuuity.data.metadata;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.data.operation.CompareAndSwap;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.ReadColumnRange;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of MetaDataStore that serializes every entry into a byte array and stores that in a column.
 */
public class SerializingMetaDataStore implements MetaDataStore {

  private static final Logger LOG = LoggerFactory.getLogger(SerializingMetaDataStore.class);

  OperationExecutor opex;

  private static final Charset charsetUTF8 = Charset.forName("UTF-8");
  private static final String rowkeyPrefix = "_metadata_";
  private static final String tableName = "meta";

  private static byte[] string2Bytes(String string) {
    return string.getBytes(charsetUTF8);
  }

  private static String bytes2String(byte[] bytes) {
    return new String(bytes, charsetUTF8);
  }

  private static byte[] makeRowKey(String account) {
    //please, cache rowkey (byte[]) based on account!
    StringBuilder str = new StringBuilder();
    str.append(rowkeyPrefix);
    str.append('\0');
    str.append(account);
    return string2Bytes(str.toString());
  }

  private static byte[] makeRowKey(MetaDataEntry meta) {
    return makeRowKey(meta.getAccount());
  }

  private static String extractAccountFromRowKey(byte[] rowkey) {
    // +1 because of appending \0 above
    int offset = rowkeyPrefix.length() + 1;
    return Bytes.toString(rowkey, offset, rowkey.length - offset);
  }

  private static byte[] makeColumnKey(String app, String type, String id) {
    StringBuilder str = new StringBuilder();
    str.append(type);
    str.append('\0');
    if (app != null) {
      str.append(app);
    }
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
    if (pos < 0) {
      return null;
    }
    int pos1 = str.indexOf('\0', pos + 1);
    if (pos1 < 0) {
      return null;
    }
    if (pos1 == pos + 1) {
      return null;
    }
    return str.substring(pos + 1, pos1);
  }

  /**
   * To avoid the overhead of creating new serializer for every call,
   * we keep a serializer for each thread in a thread local structure.
   */
  ThreadLocal<MetaDataSerializer> serializers = new ThreadLocal<MetaDataSerializer>();

  /**
   * Utility method to get or create the thread local serializer.
   */
  private MetaDataSerializer getSerializer() {
    if (this.serializers.get() == null) {
      this.serializers.set(new MetaDataSerializer());
    }
    return this.serializers.get();
  }

  @Inject
  public SerializingMetaDataStore(OperationExecutor opex) {
    this.opex = opex;
  }

  @Override
  public void add(OperationContext context, MetaDataEntry entry) throws OperationException {
    add(context, entry, true);
  }

  @Override
  public void add(OperationContext context, MetaDataEntry entry, boolean resolve) throws OperationException {
    write(context, null, entry, false, resolve);
  }

  @Override
  public void update(OperationContext context, MetaDataEntry entry) throws OperationException {
    update(context, entry, true);
  }

  @Override
  public void update(OperationContext context, MetaDataEntry entry, boolean resolve) throws OperationException {
    write(context, null, entry, true, resolve);
  }

  @Override
  public void swap(OperationContext context, MetaDataEntry expected, MetaDataEntry entry) throws OperationException {
    write(context, expected, entry, true, true);
  }

  private void write(OperationContext context, MetaDataEntry expected, MetaDataEntry entry, boolean isUpdate,
                     boolean resolve)
    throws OperationException {

    if (entry == null) {
      throw new IllegalArgumentException("entry cannot be null");
    }

    byte[] rowkey = makeRowKey(entry);
    byte[] column = makeColumnKey(entry);
    byte[] bytes;

    try {
      bytes = getSerializer().serialize(entry);
    } catch (MetaDataException e) {
      // no need to log, serializer has logged this already
      // the meta data exception is only a wrapper around the actual exception
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e.getCause());
    }

    OperationResult<Map<byte[], byte[]>> result;
    try {
      Read read = new Read(tableName, rowkey, column);
      result = opex.execute(context, read);
    } catch (OperationException e) {
      String message = String.format("Error reading meta data: %s", e.getMessage());
      LOG.error(message, e);
      throw new OperationException(e.getStatus(), message, e);
    }

    byte[] bytesRead = null;
    if (!result.isEmpty()) {
      bytesRead = result.getValue().get(column);
    }

    if (!isUpdate && bytesRead != null) {
      if (resolve && Arrays.equals(bytes, bytesRead)) {
        // a value exists but it is identical with the one to write
        return;
      }
      // a different value already exists
      String message = String.format("Meta data entry for %s already exists.", entry);
      LOG.debug(message);
      throw new OperationException(StatusCode.WRITE_CONFLICT, message);
    }
    if (isUpdate) {
      if (bytesRead == null) {
        // update expects a value to update, but none is there
        String message = String.format("Meta data entry for %s does not exist.", entry);
        LOG.debug(message);
        throw new OperationException(StatusCode.ENTRY_NOT_FOUND, message);
      }
      if (expected != null) {
        byte[] expectedBytes;
        try {
          expectedBytes = getSerializer().serialize(expected);
        } catch (MetaDataException e) {
          throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
        }
        // compare with expected
        if (!Arrays.equals(bytesRead, expectedBytes)) {
          if (resolve && Arrays.equals(bytesRead, bytes)) {
            // matches the new value -> no need to write
            return;
          }
          String message = String.format("Existing meta data entry " + "for %s does not match expected entry.", entry);
          LOG.trace(message);
          // existing entry is there but does not match expected entry
          throw new OperationException(StatusCode.WRITE_CONFLICT, message);
        }
      }
    }

    try {
      if (isUpdate && expected == null) {
        // generate a write
        Write write = new Write(tableName, rowkey, column, bytes);
        opex.commit(context, write);
      } else {
        // generate a compare-and-swap operation to make sure there is no add
        // conflict with some other thread or process
        CompareAndSwap compareAndSwap = new CompareAndSwap(tableName, rowkey, column, bytesRead, bytes);
        opex.commit(context, compareAndSwap);
      }
    } catch (OperationException e) {
      if (resolve && e.getStatus() == StatusCode.WRITE_CONFLICT) {
        // in case of write conflict, silently succeed if the conflicting
        // operation wrote the same value
        try {
          // read again, this should get the value of the conflicting write
          Read read = new Read(tableName, rowkey, column);
          result = opex.execute(context, read);
          // compare the latest value. If is the same, we are good
          if (!result.isEmpty() && Arrays.equals(bytes, result.getValue().get(column))) {
            return;
          }
        } catch (OperationException e1) {
          String message = String.format("Error reading meta data: %s", e1.getMessage());
          LOG.error(message, e1);
          throw new OperationException(e.getStatus(), message, e1);
        }
      }
      // conflict could not be resolved, or a different error
      String message = String.format("Error writing meta data: %s", e.getMessage());
      if (e.getStatus() != StatusCode.WRITE_CONFLICT) {
        LOG.error(message, e);
      }
      throw new OperationException(e.getStatus(), message, e);
    }
  }

  @Override
  public void updateField(OperationContext context, String account, String application, String type, String id,
                          String field, String value, int retries)
    throws OperationException {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    if (field.isEmpty()) {
      throw new IllegalArgumentException("field cannot be empty");
    }

    updateField(context, account, application, type, id, field, null, null, null, value, null, retries, false);
  }

  @Override
  public void updateField(OperationContext context, String account, String application, String type, String id,
                          String field, byte[] value, int retries)
    throws OperationException {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    if (field.isEmpty()) {
      throw new IllegalArgumentException("field cannot be empty");
    }

    updateField(context, account, application, type, id, null, field, null, null, null, value, retries, false);
  }

  @Override
  public void swapField(OperationContext context, String account, String application, String type, String id,
                        String field, String old, String value, int retries)
    throws OperationException {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    if (field.isEmpty()) {
      throw new IllegalArgumentException("field cannot be empty");
    }

    updateField(context, account, application, type, id, field, null, old, null, value, null, retries, true);
  }

  @Override
  public void swapField(OperationContext context, String account, String application, String type, String id,
                        String field, byte[] old, byte[] value, int retries)
    throws OperationException {
    if (field == null) {
      throw new IllegalArgumentException("field cannot be null");
    }
    if (field.isEmpty()) {
      throw new IllegalArgumentException("field cannot be empty");
    }

    updateField(context, account, application, type, id, null, field, null, old, null, value, retries, true);
  }

  private void updateField(OperationContext context, String account, String application, String type, String id,
                           String textField, String binField, String textOld, byte[] binOld, String textValue,
                           byte[] binValue, int retryAttempts, boolean doCompareAndSwap)
    throws OperationException {
    if (account == null) {
      throw new IllegalArgumentException("account cannot be null");
    }
    if (account.isEmpty()) {
      throw new IllegalArgumentException("account cannot be empty");
    }
    if (id == null) {
      throw new IllegalArgumentException("id cannot be null");
    }
    if (id.isEmpty()) {
      throw new IllegalArgumentException("id cannot be empty");
    }
    if (application != null && application.isEmpty()) {
      throw new IllegalArgumentException("application cannot be empty");
    }
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (type.isEmpty()) {
      throw new IllegalArgumentException("type cannot be empty");
    }

    byte[] rowkey = makeRowKey(account);
    byte[] column = makeColumnKey(application, type, id);

    int attempts = retryAttempts < 0 ? DEFAULT_RETRIES_ON_CONFLICT : retryAttempts;

    while (attempts >= 0) { // 0 means no retry, but of course a first attempt
      --attempts;

      // read meta data entry
      Read read = new Read(tableName, rowkey, column);
      OperationResult<Map<byte[], byte[]>> result = opex.execute(context, read);

      // throw exception if not existing
      if (result.isEmpty()) {
        throw new OperationException(StatusCode.ENTRY_NOT_FOUND, "Meta data entry does not exist.");
      }

      // get the raw (serialized) bytes of the entry
      byte[] bytes = result.getValue().get(column);
      if (bytes == null) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, "Meta data entry is null.");
      }

      // de-serialize the entry
      MetaDataEntry entry;
      try {
        entry = getSerializer().deserialize(bytes);
      } catch (MetaDataException e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
      }

      // in case of compare-and-swap, check the existing value
      if (doCompareAndSwap) {
        if (textField != null) {
          String existingValue = entry.getTextField(textField);
          if ((textOld == null && existingValue != null) || (textOld != null && !textOld.equals(existingValue))) {
            throw new OperationException(StatusCode.WRITE_CONFLICT, "Existing field value does not match expected " +
              "value");
          }
        } else if (binField != null) {
          byte[] existingValue = entry.getBinaryField(binField);
          if ((binOld == null && existingValue != null) || !Arrays.equals(binOld, existingValue)) {
            throw new OperationException(StatusCode.WRITE_CONFLICT, "Existing field value does not match expected " +
              "value");
          }
        }
      }

      // update the field
      if (textField != null) {
        entry.addField(textField, textValue);
      } else if (binField != null) {
        entry.addField(binField, binValue);
      } else {
        throw new IllegalArgumentException("Only one of textField or binField may be null");
      }

      // re-serialize
      byte[] newBytes;
      try {
        newBytes = getSerializer().serialize(entry);
        if (newBytes == null || newBytes.length == 0) {
          throw new OperationException(StatusCode.INTERNAL_ERROR, "serialization of meta data entry returned " +
            (newBytes == null ? "null" : "empty byte array"));
        }
      } catch (MetaDataException e) {
        throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
      }

      // write w/ compareAndSwap
      try {
        CompareAndSwap compareAndSwap = new CompareAndSwap(tableName, rowkey, column, bytes, newBytes);
        opex.commit(context, compareAndSwap);
        return;
      } catch (OperationException e) {
        if (e.getStatus() == StatusCode.WRITE_CONFLICT && attempts >= 0) {
          continue;
        }
        throw e;
      }
    }
  }

  @Override
  public MetaDataEntry get(OperationContext context, String account, String application, String type, String id)
    throws OperationException {

    if (account == null) {
      throw new IllegalArgumentException("account cannot be null");
    }
    if (account.isEmpty()) {
      throw new IllegalArgumentException("account cannot be empty");
    }
    if (id == null) {
      throw new IllegalArgumentException("id cannot be null");
    }
    if (id.isEmpty()) {
      throw new IllegalArgumentException("id cannot be empty");
    }
    if (application != null && application.isEmpty()) {
      throw new IllegalArgumentException("application cannot be empty");
    }
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (type.isEmpty()) {
      throw new IllegalArgumentException("type cannot be empty");
    }

    byte[] rowkey = makeRowKey(account);
    byte[] column = makeColumnKey(application, type, id);

    try {
      Read read = new Read(tableName, rowkey, column);
      OperationResult<Map<byte[], byte[]>> result = opex.execute(context, read);

      if (result.isEmpty()) {
        return null;
      }

      byte[] bytes = result.getValue().get(column);
      if (bytes == null) {
        return null;
      }

      return getSerializer().deserialize(bytes);

    } catch (OperationException e) {
      String message = String.format("Error reading meta data: %s", e.getMessage());
      LOG.error(message, e);
      throw new OperationException(e.getStatus(), message, e);

    } catch (MetaDataException e) {
      throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  @Override
  public void delete(OperationContext context, String account, String application, String type, String id)
    throws OperationException {

    if (account == null) {
      throw new IllegalArgumentException("account cannot be null");
    }
    if (account.isEmpty()) {
      throw new IllegalArgumentException("account cannot be empty");
    }
    if (id == null) {
      throw new IllegalArgumentException("id cannot be null");
    }
    if (id.isEmpty()) {
      throw new IllegalArgumentException("id cannot be empty");
    }
    if (application != null && application.isEmpty()) {
      throw new IllegalArgumentException("application cannot be empty");
    }
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (type.isEmpty()) {
      throw new IllegalArgumentException("type cannot be empty");
    }

    byte[] rowkey = makeRowKey(account);
    byte[] column = makeColumnKey(application, type, id);

    try {
      opex.commit(context, new Delete(tableName, rowkey, column));

    } catch (OperationException e) {
      String message = String.format("Error deleting meta data: %s", e.getMessage());
      LOG.error(message, e);
      throw new OperationException(e.getStatus(), message, e);
    }
  }

  @Override
  public List<MetaDataEntry> list(OperationContext context, String account, String application, String type,
                                  Map<String, String> fields)
    throws OperationException {
    try {
      if (account == null) {
        throw new IllegalArgumentException("account cannot be null");
      }
      if (account.isEmpty()) {
        throw new IllegalArgumentException("account cannot be empty");
      }
      if (application != null && application.isEmpty()) {
        throw new IllegalArgumentException("application cannot be empty");
      }
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      if (type.isEmpty()) {
        throw new IllegalArgumentException("type cannot be empty");
      }

      byte[] rowkey = makeRowKey(account);
      byte[] start = startColumnKey(application, type);
      byte[] stop = stopColumnKey(application, type);
      ReadColumnRange read = new ReadColumnRange(tableName, rowkey, start, stop);
      OperationResult<Map<byte[], byte[]>> result = opex.execute(context, read);

      if (result.isEmpty()) {
        return Collections.emptyList();
      }

      List<MetaDataEntry> entries = Lists.newArrayList();
      for (byte[] bytes : result.getValue().values()) {
        MetaDataEntry meta;
        try {
          meta = getSerializer().deserialize(bytes);
        } catch (MetaDataException e) {
          throw new OperationException(StatusCode.INTERNAL_ERROR, e.getMessage(), e.getCause());
        }
        if (!type.equals(meta.getType())) {
          continue;
        }
        if (application != null && !application.equals(meta.getApplication())) {
          continue;
        }

        if (fields != null) {
          boolean match = true;
          for (Map.Entry<String, String> field : fields.entrySet()) {
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
          if (!match) {
            continue;
          }
        }
        entries.add(meta);
      }

      return entries;

    } catch (OperationException e) {
      String message = String.format("Error reading meta data: %s", e.getMessage());
      LOG.error(message, e);
      throw new OperationException(e.getStatus(), message, e);
    }
  }

  @Override
  public void clear(OperationContext context, String account, String application) throws OperationException {

    if (account == null) {
      throw new IllegalArgumentException("account cannot be null");
    }
    if (account.isEmpty()) {
      throw new IllegalArgumentException("account cannot be empty");
    }
    if (application != null && application.isEmpty()) {
      throw new IllegalArgumentException("application cannot be empty");
    }

    byte[] rowkey = makeRowKey(account);
    ReadColumnRange read = new ReadColumnRange(tableName, rowkey, null, null);
    OperationResult<Map<byte[], byte[]>> result;
    try {
      result = opex.execute(context, read);
    } catch (OperationException e) {
      String message = String.format("Error reading meta data: %s", e.getMessage());
      LOG.error(message, e);
      throw new OperationException(e.getStatus(), message, e);
    }

    byte[][] columns;
    Set<byte[]> colset = result.getValue().keySet();
    if (application == null) {
      columns = colset.toArray(new byte[colset.size()][]);
    } else {
      List<byte[]> cols = Lists.newArrayList();
      for (byte[] colkey : colset) {
        if (application.equals(extractApplication(colkey))) {
          cols.add(colkey);
        }
      }
      columns = cols.toArray(new byte[cols.size()][]);
    }

    Delete delete = new Delete(tableName, rowkey, columns);
    try {
      opex.commit(context, delete);
    } catch (OperationException e) {
      String message = String.format("Error clearing meta data: %s", e.getMessage());
      LOG.error(message, e);
      throw new OperationException(e.getStatus(), message, e);
    }
  }

  @Override
  public Collection<String> listAccounts(OperationContext context) throws OperationException {
    // Integer.MAX_VALUE basically means "give me all" here ;)
    ReadAllKeys readAllKeysOperation = new ReadAllKeys(tableName, 0, Integer.MAX_VALUE);
    OperationResult<List<byte[]>> result = opex.execute(context, readAllKeysOperation);
    List<String> accounts = Lists.newArrayList();
    if (!result.isEmpty()) {
      for (byte[] row : result.getValue()) {
        accounts.add(extractAccountFromRowKey(row));
      }
    }

    return accounts;
  }
}
