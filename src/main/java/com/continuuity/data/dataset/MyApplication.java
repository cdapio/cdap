package com.continuuity.data.dataset;

import com.continuuity.api.data.set.IndexedTable;
import com.continuuity.api.data.set.KeyValueTable;

public class MyApplication {

  public interface IndexedTableFactory {
    IndexedTable create(String name, byte[] bytes);
  }

  public ApplicationSpec configure() {

    // a key value table mapping name to phone number
    KeyValueTable numbers = new KeyValueTable("phoneTable");

    // an indexed tables allowing forward and reverse lookup
    IndexedTable idxNumbers = new IndexedTable("phoneIndex",
        new byte[] { 'p', 'h', 'o', 'n', 'e' });

    return new ApplicationSpec().
        name("phoneNumbers").
        dataset(numbers).
        dataset(idxNumbers);
  }

}

