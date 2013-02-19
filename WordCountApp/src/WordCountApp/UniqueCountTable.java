package WordCountApp;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

import java.util.Map;

/**
 * Counts the number of unique entries seen given any number of entries.
 */
public class UniqueCountTable extends DataSet {

  public static final TupleSchema UNIQUE_COUNT_TABLE_TUPLE_SCHEMA =
      new TupleSchemaBuilder()
          .add("entry", String.class)
          .add("count", Long.class)
          .create();

  private Table uniqueCountTable;
  private Table entryCountTable;

  public UniqueCountTable(String name) {
    super(name);
    this.uniqueCountTable = new Table("unique_count_" + name);
    this.entryCountTable = new Table("entry_count_" + name);
  }

  public UniqueCountTable(DataSetSpecification spec) {
    super(spec);
    this.uniqueCountTable = new Table(
        spec.getSpecificationFor("unique_count_" + this.getName()));
    this.entryCountTable = new Table(
        spec.getSpecificationFor("entry_count_" + this.getName()));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
        .dataset(this.uniqueCountTable.configure())
        .dataset(this.entryCountTable.configure())
        .create();
  }

  /** Row and column names used for storing the unique count */
  private static final byte [] UNIQUE_COUNT = Bytes.toBytes("unique");
  
  /** Column name used for storing count of each entry */
  private static final byte [] ENTRY_COUNT = Bytes.toBytes("count");

  /**
   * Returns the current unique count.
   * @return current number of unique entries
   * @throws OperationException
   */
  public Long readUniqueCount() throws OperationException {
    OperationResult<Map<byte[], byte[]>> result =
        this.uniqueCountTable.read(new Read(UNIQUE_COUNT, UNIQUE_COUNT));
    if (result.isEmpty()) return 0L;
    byte [] countBytes = result.getValue().get(UNIQUE_COUNT);
    if (countBytes == null || countBytes.length != 8) return 0L;
    return Bytes.toLong(countBytes);
  }

  /**
   * Adds the specified entry to the table and augments the specified Tuple with
   * a special field that will be used in the downstream Flowlet this Tuple is
   * sent to.
   * <p>
   * Continuously add entries into the table using this method, pass the Tuple
   * to another downstream Flowlet, and in the second Flowlet pass the Tuple to
   * the {@link #updateUniqueCount(Tuple)}.
   * @param entry entry to add
   * @return tuple that will be passed
   * @throws OperationException
   */
  public Tuple writeEntryAndCreateTuple(String entry)
      throws OperationException {
    Long newCount = this.entryCountTable.
      increment(new Increment(Bytes.toBytes(entry), ENTRY_COUNT, 1L)).
      get(ENTRY_COUNT);
    return new TupleBuilder()
      .set("entry", entry)
      .set("count", newCount)
      .create();
  }

  /**
   * Updates the unique count based on the passed-through Tuple from the
   * upstream Flowlet.
   * @param tuple
   * @throws OperationException
   */
  public void updateUniqueCount(Tuple tuple)
      throws OperationException {
    Long count = tuple.get("count");
    if (count == 1L) {
      this.uniqueCountTable.write(
          new Increment(UNIQUE_COUNT, UNIQUE_COUNT, 1L));
    }
  }
}
