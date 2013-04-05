package com.payvment.continuuity.data;

import java.util.Map;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.google.gson.Gson;

public class ObjectTable extends DataSet {

  private static final byte [] COLUMN = new byte [] { 'c' };

  private final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
    @Override
    protected Gson initialValue() {
      return new Gson();
    }
  };

  private final Table table;

  public ObjectTable(String name) {
    super(name);
    this.table = new Table("o_" + name);
  }

  public ObjectTable(DataSetSpecification spec) {
    super(spec);
    this.table = new Table("o_" + this.getName());
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .dataset(this.table.configure())
      .create();
  }

  public <T> void put(String key, T o) throws OperationException {
    String s = this.gson.get().toJson(o);
    this.table.write(new Write(Bytes.toBytes(key), COLUMN,
        Bytes.toBytes(s)));
  }

  public <T> T get(String key, Class<T> keyClass) throws OperationException {
    OperationResult<Map<byte[], byte[]>> result
    = this.table.read(new Read(Bytes.toBytes(key), COLUMN));
    if (result == null || result.isEmpty()) {
      return null;
    }
    byte[] value = result.getValue().get(key);
    return (this.gson.get().fromJson(new String(value), keyClass));
  }
}