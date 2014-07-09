package com.continuuity.api.dataset.lib;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * An ObjectStore Dataset extension that supports access to objects via indices; lookups by the index will return
 * all the objects stored in the object store that have the index value.
 *
 * The dataset uses two tables: an object store, to store the actual data, and a second table for the index.
 *
 * @param <T> the type of objects in the store
 */
public class IndexedObjectStore<T> extends AbstractDataset {
  //NOTE: cannot use byte[0] as empty value because byte[0] is treated as null
  private static final byte[] EMPTY_VALUE = new byte[1];
  //KEY_PREFIX is used to prefix primary key when it stores PrimaryKey -> Categories mapping.
  private static final byte[] KEY_PREFIX = Bytes.toBytes("_fk");

  //IndexedObjectStore stores the following mappings
  // 1. ObjectStore
  //    (primaryKey to Object)
  // 2. Index table
  //    (secondaryKeys to primaryKey mapping)
  //    (prefixedPrimaryKey to secondaryKeys)
  private final ObjectStore<T> objectStore;
  private final Table index;

  /**
   * Constructs the IndexedObjectStore with name and type.
   *
   * @param name name of the dataset
   * @param objectStore dataset to use as the objectStore
   * @param index dataset to use as the index
   */
  public IndexedObjectStore(String name, ObjectStore<T> objectStore, Table index) {
    super(name, objectStore, index);
    this.objectStore = objectStore;
    this.index = index;
  }

  /**
   * Read all the objects from the objectStore for a given index. Returns all the objects that match the secondaryKey.
   * Returns an empty list if no values are found. Never returns null.
   *
   * @param secondaryKey for the lookup.
   * @return List of Objects matching the secondaryKey.
   */
  public List<T> readAllByIndex(byte[] secondaryKey) {
    ImmutableList.Builder<T> resultList = ImmutableList.builder();
    //Lookup the secondaryKey and get all the keys in primary
    //Each row with secondaryKey as rowKey contains column named as the primary key
    // of every object that can be looked up using the secondaryKey
    Row row = index.get(secondaryKey);

    // if the index has no match, return nothing
    if (!row.isEmpty()) {
      for (byte[] column : row.getColumns().keySet()) {
        T obj = objectStore.read(column);
        resultList.add(obj);
      }
    }
    return resultList.build();
  }

  private List<byte[]> secondaryKeysToDelete(Set<byte[]> existingSecondaryKeys, Set<byte[]> newSecondaryKeys) {
    List<byte[]> secondaryKeysToDelete = Lists.newArrayList();
    if (existingSecondaryKeys.size() > 0) {
      for (byte[] secondaryKey : existingSecondaryKeys) {
        // If it is not in newSecondaryKeys then it needs to be deleted.
        if (!newSecondaryKeys.contains(secondaryKey)) {
          secondaryKeysToDelete.add(secondaryKey);
        }
      }
    }
    return secondaryKeysToDelete;
  }

  private List<byte[]> secondaryKeysToAdd(Set<byte[]> existingSecondaryKeys, Set<byte[]> newSecondaryKeys) {
    List<byte[]> secondaryKeysToAdd = Lists.newArrayList();
    if (existingSecondaryKeys.size() > 0) {
      for (byte[] secondaryKey : newSecondaryKeys) {
        // If it is not in existingSecondaryKeys then it needs to be added
        // else it exists already.
        if (!existingSecondaryKeys.contains(secondaryKey)) {
          secondaryKeysToAdd.add(secondaryKey);
        }
      }
    } else {
      //all the newValues should be added
      secondaryKeysToAdd.addAll(newSecondaryKeys);
    }

    return secondaryKeysToAdd;
  }

  /**
   * Writes to the dataset, deleting any existing secondaryKey corresponding to the key and updates the indexTable with
   * the secondaryKey that is passed.
   *
   * @param key key for storing the object
   * @param object object to be stored
   * @param secondaryKeys indices that can be used to lookup the object
   */
  public void write(byte[] key, T object, byte[][] secondaryKeys) {
    writeToObjectStore(key, object);

    //Update the secondaryKeys
    //logic:
    //  - Get existing secondary keys
    //  - Compute diff between existing secondary keys and new secondary keys
    //  - Remove the secondaryKeys that are removed
    //  - Add the new keys that are added
    Row row = index.get(getPrefixedPrimaryKey(key));
    Set<byte[]> existingSecondaryKeys = Sets.newTreeSet(new Bytes.ByteArrayComparator());

    if (!row.isEmpty()) {
      existingSecondaryKeys = row.getColumns().keySet();
    }

    Set<byte[]> newSecondaryKeys = new TreeSet<byte[]>(new Bytes.ByteArrayComparator());
    newSecondaryKeys.addAll(Arrays.asList(secondaryKeys));

    List<byte[]> secondaryKeysDeleted = secondaryKeysToDelete(existingSecondaryKeys, newSecondaryKeys);
    if (secondaryKeysDeleted.size() > 0) {
      deleteSecondaryKeys(key, secondaryKeysDeleted.toArray(new byte[secondaryKeysDeleted.size()][]));
    }

    List<byte[]> secondaryKeysAdded =  secondaryKeysToAdd(existingSecondaryKeys, newSecondaryKeys);

    //for each key store the secondaryKey. This will be used while deleting old index values.
    if (secondaryKeysAdded.size() > 0) {
      byte[][] fooValues = new byte[secondaryKeysAdded.size()][];
      Arrays.fill(fooValues, EMPTY_VALUE);
      index.put(getPrefixedPrimaryKey(key),
                 secondaryKeysAdded.toArray(new byte[secondaryKeysAdded.size()][]),
                 fooValues);
    }

    for (byte[] secondaryKey : secondaryKeysAdded) {
      //update the index.
      index.put(secondaryKey, key, EMPTY_VALUE);
    }
  }

  private void writeToObjectStore(byte[] key, T object) {
    objectStore.write(key, object);
  }

  public void write(byte[] key, T object) {
    Row row = index.get(getPrefixedPrimaryKey(key));
    if (!row.isEmpty()) {
      Set<byte[]> columnsToDelete = row.getColumns().keySet();
      deleteSecondaryKeys(key, columnsToDelete.toArray(new byte[columnsToDelete.size()][]));
    }
    writeToObjectStore(key, object);
  }


  private void deleteSecondaryKeys(byte[] key, byte[][] columns) {
    //Delete the key to secondaryKey mapping
    index.delete(getPrefixedPrimaryKey(key), columns);

    // delete secondaryKey to key mapping
    for (byte[] col : columns) {
      index.delete(col, key);
    }
  }

  private byte[] getPrefixedPrimaryKey(byte[] key) {
    return Bytes.add(KEY_PREFIX, key);
  }

  /**
   * Deletes an index that is no longer needed. After deleting the index, lookups using the index value will no
   * longer return the object.
   *
   * @param key key for the object
   * @param secondaryKey index to be pruned
   */
  public void pruneIndex(byte[] key, byte[] secondaryKey) {
    this.index.delete(secondaryKey, key);
    this.index.delete(getPrefixedPrimaryKey(key), secondaryKey);
  }

  /**
   * Updates the index value for an existing key. This will not delete the older secondaryKeys.
   *
   * @param key key for the object
   * @param secondaryKey index to be pruned
   */
  public void updateIndex(byte[] key, byte[] secondaryKey) {
    this.index.put(secondaryKey, key, EMPTY_VALUE);
    this.index.put(getPrefixedPrimaryKey(key), secondaryKey, EMPTY_VALUE);
  }
}
