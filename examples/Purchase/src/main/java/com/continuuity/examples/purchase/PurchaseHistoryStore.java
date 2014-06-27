/**
 * Copyright 2013-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.examples.purchase;

import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.RecordScannable;
import com.continuuity.api.data.batch.RecordScanner;
import com.continuuity.api.data.batch.Scannables;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.lib.ObjectStore;
import com.continuuity.api.dataset.lib.ObjectStores;
import com.continuuity.api.dataset.module.EmbeddedDataset;
import com.continuuity.internal.io.UnsupportedTypeException;

import java.lang.reflect.Type;
import java.util.List;

/**
 * This stores purchase histories in an embedded object store. Embedding the object store into this dataset
 * ensures that the type parameter (the PurchaseHistory class) is bundled with this dataset's code when it is
 * deployed. That means a PurchaseHistoryStore can be used outside of this application, particularly for
 * querying the dataset with SQL.
 *
 * This class implements BatchWritable in order to be able to write to it from Map/Reduce, and RecordScannable
 * in order to run ad-hoc queries against it.
 */
public class PurchaseHistoryStore
  extends AbstractDataset
  implements RecordScannable<PurchaseHistory>, BatchWritable<String, PurchaseHistory> {

  // the embedded object store
  private final ObjectStore<PurchaseHistory> store;

  /**
   * These properties will be required to create the object store. We provide them here through a static
   * method such that application code that uses this dataset does not need to be aware of the detailed
   * properties that are expected.
   *
   * @return the properties required to create an instance of this dataset
   */
  public static DatasetProperties properties() {
    try {
      return ObjectStores.objectStoreProperties(PurchaseHistory.class, DatasetProperties.EMPTY);
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException("This should never be thrown - PurchaseHistory is a supported type", e);
    }
  }

  /**
   * Constructor from a specification and the embedded object store. By convention,
   * implementing this constructor allows to define this dataset type without an explicit DatasetDefinition.
   *
   * @param spec the specification
   * @param objStore the embedded object store
   */
  public PurchaseHistoryStore(DatasetSpecification spec,
                              @EmbeddedDataset("store") ObjectStore<PurchaseHistory> objStore) {
    super(spec.getName(), objStore);
    this.store = objStore;
  }

  @Override // RecordScannable
  public Type getRecordType() {
    return PurchaseHistory.class;
  }

  @Override // RecordScannable
  public List<Split> getSplits() {
    return store.getSplits();
  }

  @Override // RecordScannable
  public RecordScanner<PurchaseHistory> createSplitRecordScanner(Split split) {
    return Scannables.valueRecordScanner(store.createSplitReader(split));
  }

  @Override // BatchWritable
  public void write(String key, PurchaseHistory history) {
    store.write(key, history);
  }

  /**
   * Write a purchase history to the store. Uses the customer field of the purchase history as the key.
   *
   * @param history The purchase history to store.
   */
  public void write(PurchaseHistory history) {
    store.write(history.getCustomer(), history);
  }

  /**
   * @param customer the customer in question
   * @return the purchase history of the given customer
   */
  public PurchaseHistory read(String customer) {
    return store.read(customer);
  }
}
