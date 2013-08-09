package com.continuuity.data.table;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.KeyRange;
import com.continuuity.data.operation.executor.ReadPointer;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This adds ordering of keys to the versioned columnar tables. Keys are ordered in ascending lexicographic byte
 * order. The table (or a subrange of the table) can then be scanned in the order of the keys.
 */
public interface OrderedVersionedColumnarTable extends VersionedColumnarTable {

  /**
   * Scans the table and returns all row keys according to the specified
   * limit and offset and respecting the specified read pointer.
   * @param limit
   * @param offset
   * @param readPointer
   * @return list of keys
   */
  public List<byte[]> getKeys(int limit, int offset, ReadPointer readPointer) throws OperationException;

  /**
   * Scans all columns of all rows between the specified start row (inclusive)
   * and stop row (exclusive).  Returns the latest visible version of each
   * column. If startRow is null the scan starts reading from first available entry.
   * If stopRow is null, the scan ends until all entries in table are read.
   * @param startRow
   * @param stopRow
   * @param readPointer
   * @return scanner cursor
   */
  public Scanner scan(byte[] startRow, byte[] stopRow,
      ReadPointer readPointer);


  /**
   * Scans the specified columns of all rows between the specified start row
   * (inclusive) and stop row (exclusive).  Returns the latest visible version
   * of each column. If startRow is null the scan starts reading from first available entry.
   * If stopRow is null, the scan ends until all entries in table are read.
   * @param startRow
   * @param stopRow
   * @param columns
   * @param readPointer
   * @return scanner cursor
   */
  public Scanner scan(byte[] startRow, byte[] stopRow,
      byte[][] columns, ReadPointer readPointer);


  /**
   * Scans all columns of all rows.  Returns the latest visible version of each
   * column.
   * @param readPointer
   * @return scanner cursor
   */
  public Scanner scan(ReadPointer readPointer);

  /**
   * Returns a list of key ranges that partition the table into approximately evenly sized ranges. If a start or a
   * stop row key are given, then they limit the range covered by the partitions. If a list of columns is given,
   * then they are a hint for the partitioning that only these columns will be read. If a number of splits is
   * explicitly requested, then the number of returned ranges will not exceed that number.
   * @param numSplits the desired number of splits, or -1 to leave it up to the partitioner
   * @param start the start of the range to be covered by the splits, or null to start with the least key in the table
   * @param stop the end of the range to be covered by the splits, exclusively, or null to read to the end of the table
   * @param columns if non-null, a hint for the partitioner that only these columns are needed.
   * @param pointer the read pointer under which splits should be visible.
   * @return A list of key ranges.
   * @throws OperationException in case of error
   */
  public List<KeyRange> getSplits(int numSplits, byte[] start, byte[] stop, byte[][] columns, ReadPointer pointer)
    throws OperationException;

  /**
   * Delete all rows in a given range completely, and dirtily, that is, bypassing transactions: these rows will
   * disappear right away, and even current transactions will not be able to read them anymore.
   * @param startRow the first row to delete
   * @param stopRow the first row not to delete, or null to delete all rows greater or equal to startRow.
   * @throws OperationException in case of error
   */
  public void deleteRowsDirtily(byte[] startRow, @Nullable byte[] stopRow) throws OperationException;

  /**
   * Delete all rows with a given prefix completely, and dirtily, that is, bypassing transactions: these rows will
   * disappear right away, and even current transactions will not be able to read them anymore.
   * @param prefix the prefix of rows to delete
   * @throws OperationException in case of error
   */
  public void deleteRowsDirtily(byte[] prefix) throws OperationException;

}
