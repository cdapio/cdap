package com.continuuity.data2.dataset.lib.table;

import com.continuuity.data.table.Scanner;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

/**
 * A table interface dedicated to our metrics system.
 */
public interface MetricsTable {

  void put(Map<byte[], Map<byte[], byte[]>> updates) throws Exception;
  void increment(byte[] row, Map<byte[], Long> increments) throws Exception;
  void deleteAll(byte[] prefix) throws Exception;
  void delete(Collection<byte[]> rows) throws Exception;
  Scanner scan(@Nullable byte[] start, @Nullable byte[] stop, @Nullable byte[][] columns,
               @Nullable FuzzyRowFilter filter) throws Exception;

}

