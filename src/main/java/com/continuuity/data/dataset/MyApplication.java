package com.continuuity.data.dataset;

public class MyApplication {

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

