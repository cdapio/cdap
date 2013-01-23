package com.continuuity.api.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;

import java.util.Map;

public class KeyValueTable extends DataSet {

  static final byte[] KEY_COLUMN = { 'k', 'e', 'y' };

  private Table table;

  public KeyValueTable(String name) {
    super(name);
    this.table = new Table("kv_" + name);
  }

  @SuppressWarnings("unused")
  public KeyValueTable(DataSetSpecification spec) throws OperationException {
    super(spec);
    this.table = new Table(spec.getSpecificationFor("kv_" + this.getName()));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).
        dataset(this.table.configure()).create();
  }

  public byte[] read(byte[] key) throws OperationException {
    OperationResult<Map<byte[], byte[]>> result =
        this.table.read(new Read(key, KEY_COLUMN));
    if (result.isEmpty()) {
      return null;
    } else {
      return result.getValue().get(KEY_COLUMN);
    }
  }

  public void write(byte[] key, byte[] value) throws OperationException {
    this.table.exec(new Write(key, KEY_COLUMN, value));
  }

  public void stage(byte[] key, byte[] value) throws OperationException {
    this.table.stage(new Write(key, KEY_COLUMN, value));
  }
}
