package com.continuuity.data.metadata;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.google.common.collect.Lists;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SimpleMetaDataStore implements MetaDataStore {

  private static final Logger Log =
      LoggerFactory.getLogger(SimpleMetaDataStore.class);

  private static final String prefix = "_metadata_";
  private static final byte[] typeColumn = "type".getBytes();

  OperationExecutor opex;

  public SimpleMetaDataStore(OperationExecutor opex) {
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
    byte[] key = (prefix + entry.getName()).getBytes();
    Read read = new Read(key);
    try {
      OperationResult<Map<byte[], byte[]>> result = opex.execute(read);
      if (isUpdate && result.isEmpty()) {
        throw new MetaDataException("Meta data entry does not exist.");
      }
      else if (!isUpdate && !result.isEmpty()) {
        throw new MetaDataException("Meta data entry already exists.");
      }
    } catch (OperationException e) {
      String message =
          String.format("Error reading meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }
    List<byte[]> columns = Lists.newArrayList();
    List<byte[]> values = Lists.newArrayList();
    columns.add(typeColumn);
    values.add(entry.getType().getBytes());
    for (Map.Entry<String, String> pair : entry.getFields()) {
      columns.add(pair.getKey().getBytes());
      values.add(pair.getValue().getBytes());
    }
    byte[][] cols = new byte[columns.size()][];
    byte[][] vals = new byte[values.size()][];
    Write write = new Write(key, columns.toArray(cols), values.toArray(vals));
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
  public MetaDataEntry get(String name) throws MetaDataException {
    byte[] key = (prefix + name).getBytes();
    Read read = new Read(key);
    try {
      OperationResult<Map<byte[], byte[]>> result = opex.execute(read);
      if (result.isEmpty()) return null;
      byte[] typeBytes = result.getValue().get(typeColumn);
      if (typeBytes == null) {
        Log.error("Found a meta data entry without a type. ");
        throw new MetaDataException("Found a meta data entry without a type.");
      }
      MetaDataEntry entry = new MetaDataEntry(name, new String(typeBytes));
      for (byte[] column : result.getValue().keySet()) {
        if (Arrays.equals(column, typeColumn)) continue;
        byte[] value = result.getValue().get(column);
        if (value == null) {
          Log.warn("Found a meta data entry with null value for field "
              + new String(column) + ".");
          continue;
        }
        entry.addField(new String(column), new String(value));
      }
      return entry;
    } catch (OperationException e) {
      String message =
          String.format("Error reading meta data: %s", e.getMessage());
      Log.error(message, e);
      throw new MetaDataException(message, e);
    }
  }

  @Override
  public List<MetaDataEntry> list(String type) {
    throw new NotImplementedException();
  }

  @Override
  public List<MetaDataEntry> find(Map<String, String> fields) {
    throw new NotImplementedException();
  }
}
