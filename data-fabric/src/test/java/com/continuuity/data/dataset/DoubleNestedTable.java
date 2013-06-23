package com.continuuity.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;

// this class only used to test data set metrics name propagation
class DoubleNestedTable extends DataSet {

  private Table t;
  private KeyValueTable kv;

  public DoubleNestedTable(String name) {
    super(name);
    t = new Table(name + "-table");
    kv = new KeyValueTable(name + "-keyvalue");
  }

  public DoubleNestedTable(DataSetSpecification spec) {
    super(spec);
    t = new Table(spec.getSpecificationFor(getName() + "-table"));
    kv = new KeyValueTable(spec.getSpecificationFor(getName() + "-keyvalue"));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).
      dataset(t.configure()).
      dataset(kv.configure()).
      create();
  }

  public Long writeAndInc(String key, long value) throws OperationException {
    t.write(new Write(Bytes.toBytes(key), Bytes.toBytes(key), Bytes.toBytes(value)));
    return kv.incrementAndGet(Bytes.toBytes(key), value);
  }
}
