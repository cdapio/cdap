package com.continuuity.api.data.dataset;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * This dataset extends ObjectStore to support access to objects via indices. Look by the index will return
 * all the objects stored in the object store that has the index value.
 *
 * The dataset uses two tables: object store - to store the actual data, and a second table for the index.
 * @param <T> the type of objects in the store
 */
public class IndexedObjectStore<T> extends ObjectStore<T> {

  //IndexedObjectStore stores the following mappings
  // 1. MultiObjectStore
  //    (primaryKey to Object)
  // 2. Index table
  //    (indexValues to keyMapping)
  //    (prefixedPrimaryKey to indexValues)
  private Table index;
  private String indexName;

  private static final byte[] EMPTY_VALUE = new byte[0];
  //KEY_PREFIX is used to prefix primary key when it stores PrimaryKey -> Categories mapping.
  private static final byte[] KEY_PREFIX = Bytes.toBytes("_keyToIndexValues");

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
    ImmutableList.Builder resultList = new ImmutableList.Builder();
    //Lookup the index and get all the keys in primary
    Read idxRead = new Read(indexValue, null, null);
    OperationResult<Map<byte[], byte[]>> result = this.index.read(idxRead);

    // if the index has no match, return nothing
    if (!result.isEmpty()) {
      for (byte[] column : result.getValue().keySet()) {
        T obj = read(column);
        resultList.add(obj);
      }
    }
    return resultList.build();
  }

  private List<byte[]> indexValuesToDelete(Set<byte[]> existingIndexValues, Set<byte[]> newIndexValues) {
    List<byte[]> indexValuesToDelete = Lists.newArrayList();
    if (existingIndexValues.size() > 0) {
      for (byte[] indexValue : existingIndexValues) {
        // If it is not in newIndexValues then it needs to be deleted.
        if (!newIndexValues.contains(indexValue)) {
          indexValuesToDelete.add(indexValue);
        }
      }
    }
    return indexValuesToDelete;
  }

  private List<byte[]> indexValuesToAdd(Set<byte[]> existingIndexValues, Set<byte[]> newIndexValues) {
    List<byte[]> indexValuesToAdd = Lists.newArrayList();
    if (existingIndexValues.size() > 0) {
      for (byte[] indexValue : newIndexValues) {
        // If it is not in existingIndexValues then it needs to be added
        // else it exists already.
        if (!existingIndexValues.contains(indexValue)) {
          indexValuesToAdd.add(indexValue);
        }
      }
    } else {
      //all the newValues should be added
      indexValuesToAdd.addAll(newIndexValues);
    }

    return indexValuesToAdd;
  }

  /**
   * Write to the data set, deletes older indexValues corresponding the key and updates the indexTable with the
   * indexValues that is passed.
   * @param key key for storing the object.
   * @param object object to be stored.
   * @param indexValues indices that can be used to lookup the object.
   * @throws OperationException incase of errors.
   */
  public void write(byte[] key, T object, byte[][] indexValues) throws OperationException {

    writeToObjectStore(key, object);

    //Update the indexValues
    OperationResult<Map<byte[], byte[]>> result = index.read(new Read(getPrefixedPrimaryKey(key)));
    Set<byte[]> existingIndexValues = Sets.newTreeSet(new Bytes.ByteArrayComparator());

    if (!result.isEmpty()){
      existingIndexValues = result.getValue().keySet();
    }

    Set<byte[]> newIndexValues = new TreeSet<byte[]>(new Bytes.ByteArrayComparator());
    newIndexValues.addAll(Arrays.asList(indexValues));

    List<byte[]> indexValuesDeleted = indexValuesToDelete(existingIndexValues, newIndexValues);
    deleteIndexValues(key, indexValuesDeleted.toArray(new byte[indexValuesDeleted.size()][]));

    List<byte[]> indexValuesAdded =  indexValuesToAdd(existingIndexValues, newIndexValues);
    index.write(new Write(getPrefixedPrimaryKey(key),
                          indexValuesAdded.toArray(new byte[indexValuesAdded.size()][]),
                          new byte[indexValuesAdded.size()][0]));

    for (byte[] indexValue : indexValues) {
      //update the index.
      index.write(new Write(indexValue, key, EMPTY_VALUE));
      //for each key store the indexValue of the key. This will be used while deleting old index values.
      index.write(new Write(getPrefixedPrimaryKey(key), indexValue, EMPTY_VALUE));
    }
  }

  private void writeToObjectStore(byte[] key, T object) throws OperationException {
    super.write(key, object);
  }

  @Override
  public void write(byte[] key, T object) throws OperationException {
    OperationResult<Map<byte[], byte[]>> existingIndexValues = index.read(new Read(getPrefixedPrimaryKey(key)));
    if (!existingIndexValues.isEmpty() && existingIndexValues.getValue().size() > 0){
      Set<byte[]> columnsToDelete = existingIndexValues.getValue().keySet();
      deleteIndexValues(key, columnsToDelete.toArray(new byte[columnsToDelete.size()][]));
    }
    writeToObjectStore(key, object);
  }


  private void deleteIndexValues(byte[] key, byte[][] columns) throws OperationException{
    //Delete the key to indexValue mapping
    index.write(new Delete(getPrefixedPrimaryKey(key), columns));

    // delete indexValue to key mapping
    for (byte[] col : columns){
      index.write(new Delete(col, key));
    }
  }

  private byte[] getPrefixedPrimaryKey(byte[] key){
    return Bytes.add(KEY_PREFIX, key);
  }

  /**
   * Delete an index that is no longer needed. After deleting the index the lookup using the index value will no
   * longer return the object.
   * @param key key for the object.
   * @param indexValue index to be pruned.
   * @throws OperationException incase of errors.
   */
  public void pruneIndex(byte[] key, byte[] indexValue) throws OperationException {
    this.index.write(new Delete(indexValue, key));
    this.index.write(new Delete(getPrefixedPrimaryKey(key), indexValue));
  }

  /**
   * Update index value for an existing key. This will not delete the old indexValues.
   * @param key key for the object.
   * @param indexValue index to be pruned.
   * @throws OperationException incase of errors.
   */
  public void updateIndex(byte[] key, byte[] indexValue) throws OperationException {
    this.index.write(new Write(indexValue, key, EMPTY_VALUE));
    this.index.write(new Write(getPrefixedPrimaryKey(key), indexValue, EMPTY_VALUE));
  }
}
