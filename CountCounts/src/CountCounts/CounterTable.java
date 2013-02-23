package CountCounts;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;

/**
 *
 */
public class CounterTable extends DataSet {

  KeyValueTable kvTable;

  public CounterTable(String name) {
    super(name);
    kvTable = new KeyValueTable("x_" + this.getName());
  }

  public CounterTable(DataSetSpecification spec) {
    super(spec);
    kvTable = new KeyValueTable(spec.getSpecificationFor("x_" + this.getName()));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .dataset(this.kvTable.configure())
      .create();
  }

  public long get(String key) throws OperationException {
    byte[] value = this.kvTable.read(key.getBytes());
    if (value == null) {
      return 0L;
    } else if (value.length == Bytes.SIZEOF_LONG) {
        return Bytes.toLong(value);
    } else {
      return -1L;
    }
  }

  public void increment(String key) {
    try {
      this.kvTable.increment(key.getBytes(), 1L);
    } catch (OperationException e) {
      throw new RuntimeException(e);
    }
  }
}
