package com.continuuity.api.data.dataset.table;

import java.util.Map;

/**
 * Represents one row in a table.
 */
public interface Row {
  byte[] getRow();
  Map<byte[], byte[]> getColumns();
}
