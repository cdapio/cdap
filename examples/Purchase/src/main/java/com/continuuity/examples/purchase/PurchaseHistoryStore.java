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
import com.continuuity.api.dataset.module.EmbeddedDataSet;
import com.continuuity.internal.io.UnsupportedTypeException;

import java.lang.reflect.Type;
import java.util.List;

/**
 *
 */
public class PurchaseHistoryStore
  extends AbstractDataset
  implements RecordScannable<PurchaseHistory>, BatchWritable<String, PurchaseHistory> {

  private final ObjectStore<PurchaseHistory> store;

  public static DatasetProperties properties() {
    try {
      return ObjectStores.objectStoreProperties(PurchaseHistory.class, DatasetProperties.EMPTY);
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException("This should never be thrown!", e);
    }
  }

  public PurchaseHistoryStore(DatasetSpecification spec,
                              @EmbeddedDataSet("store") ObjectStore<PurchaseHistory> objStore) {
    super(spec.getName(), objStore);
    this.store = objStore;
  }

  @Override
  public Type getRecordType() {
    return PurchaseHistory.class;
  }

  @Override
  public List<Split> getSplits() {
    return store.getSplits();
  }

  @Override
  public RecordScanner<PurchaseHistory> createSplitRecordScanner(Split split) {
    return Scannables.valueRecordScanner(store.createSplitReader(split));
  }

  public void write(String key, PurchaseHistory history) {
    store.write(key, history);
  }

  public void write(PurchaseHistory history) {
    store.write(history.getCustomer(), history);
  }

  public PurchaseHistory read(String customer) {
    return store.read(customer);
  }
}
