package com.continuuity.api.data.dataset;

import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.collect.Lists;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This dataset extends ObjectStore to support access to objects via indices. Look by the index will return
 * all the objects stored in the object store that has the index value.
 *
 * The dataset uses two tables: object store - to store the actual data, and a second table for the index.
 * The responsibility of pruning the stale index values lies with the user of this data set.
 *
 * @param <T> the type of objects in the store
 */
public class IndexedObjectStore<T> extends ObjectStore<T> {

  private Table index;
  private String indexName;

  private static final byte[] EXISTS = { 'x' };

  /**
   * Construct IndexObjectStore with name and type.
   * @param name name of the dataset
   * @param type type of the object stored in the dataset
   * @throws UnsupportedTypeException if the type cannot be supported
   */
  public IndexedObjectStore(String name, Type type) throws UnsupportedTypeException {
    super(name, type);
    this.init(name);
    this.index = new Table(this.indexName);
  }

  /**
   * Constructor from a data set specification.
   * @param spec the specification
   */
  public IndexedObjectStore(DataSetSpecification spec) {
    super(spec);
    this.init(spec.getName());
    this.index = new Table(spec.getSpecificationFor(this.indexName));
  }

  private void init(String name) {
    this.indexName = "i_" + name;
 }
  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(super.configure()).
      dataset(this.index.configure()).
      create();
  }

  /**
   * Read all the objects from objectStore via index. Returns all the objects that match the indexValue.
   * Returns an empty list if no value is found. Never returns null.
   * @param indexValue value for the lookup.
   * @return List of Objects matching the indexValue.
   * @throws OperationException in case of error.
   */
  public List<T> readAllByIndex(byte[] indexValue) throws OperationException {
    List<T> resultList = Lists.newArrayList();
    //Lookup the index and get all the keys in primary
    Read idxRead = new Read(indexValue, null, null);
    OperationResult<Map<byte[], byte[]>> result = this.index.read(idxRead);

    // if the index has no match, return nothing
    if (!result.isEmpty()) {
      for (byte[] column : result.getValue().keySet()) {
        if (Arrays.equals(EXISTS, result.getValue().get(column))) {
          // construct a new read with this column as the row key
          T obj = read(column);
          resultList.add(obj);
        }
      }
    }
    return resultList;
  }

  /**
   * Write to the dataset and update the corresponding indices.
   * @param key key for storing the object.
   * @param object object to be stored.
   * @param indexValues indices that can be used to lookup the object.
   * @throws OperationException incase of errors.
   */
  public void write(byte[] key, T object, byte[][] indexValues) throws OperationException {
    for (byte[] indexValue : indexValues) {
      this.index.write(new Write(indexValue, key, EXISTS));
    }
    write(key, object);
  }

  /**
   * Delete an index that is no longer needed. After deleting the index the lookup using the index value will no
   * longer return the object.
   * @param key key for the object.
   * @param index index to be pruned.
   * @throws OperationException incase of errors.
   */
  public void pruneIndex(byte[] key, byte[] indexValue) throws OperationException {
    this.index.write(new Delete(indexValue, key));
  }
}
