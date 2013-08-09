package com.continuuity.data.util;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements row-level locking - without actually holding the rows, just managing the locks.
 * <ul>
 *   <li>
 *     To obtain a lock, call lock(row). The resulting lock may be invalid. That may happen if the
 *     lock was deleted from the table by a different thread, while the current thread was waiting for
 *     the lock to become available. If the lock is invalid, the caller must discard the lock and retry.
 *   </li><li>
 *     For convenience, call validLock(row), which guarantees a valid lock. It loops until lock()
 *     returns a valid lock.
 *   </li><li>
 *     The lock can be released by calling unlock(row). This may only be called for a valid lock.
 *   </li><li>
 *     Alternatively, the lock can be unlocked and removed from the lock table at the same time. This is
 *     useful whenever there are many rows, to keep the memory footprint of lock table small. Removing the
 *     lock may result in an invalid lock returned to another thread's lock().
 *   </li>
 * </ul>
 */
public class RowLockTable {

  private final ConcurrentNavigableMap<Row, RowLock> locks = new ConcurrentSkipListMap<Row, RowLock>();

  /**
   * @return the number row locks in the table
   */
  public int size() {
    return this.locks.size();
  }

  /**
   * Locks a row and returns the lock. The lock may be invalid if another thread removed the lock while this
   * thread was waiting. The caller must check the validity of the lock and discard it if it is invalid.
   * @param row the row to lock
   * @return a lock for the row, which may be invalid.
   */
  public RowLock lock(Row row) {
    RowLock lock = this.locks.get(row);
    // if there is no lock for this row yet, attempt to create and put a new lock into the table
    if (lock == null) {
      lock = new RowLock(row);
      // if another thread has put a new lock into the table since we called get(), then we use that lock
      RowLock existing = this.locks.putIfAbsent(row, lock);
      if (existing != null) {
        lock = existing;
      }
    }
    // now attempt to lock it
    lock.lock();
    // if the lock was invalidated while we were waiting, release it
    if (!lock.isValid()) {
      lock.unlock();
    }
    // return the lock, whether it was valid or not
    return lock;
  }

  /**
   * Locks a row and returns a valid lock, by retrying until a valid one is obtained.
   * @param row the row to lock
   * @return a valid lock for the row
   */
  public RowLock validLock(Row row) {
    RowLock lock;
    while (true) {
      lock = this.locks.get(row);
      if (lock == null) {
        lock = new RowLock(row);
        RowLock existing = this.locks.putIfAbsent(row, lock);
        if (existing != null) {
          lock = existing;
        }
      }
      lock.lock();
      if (lock.isValid()) {
        return lock;
      } else {
        lock.unlock();
      }
    }
  }

  /**
   * Release the lock for a row. This may only be called if the caller has the valid lock for the row.
   * This method keeps the lock in the table. It should be used if there are few rows, or if it anticipated
   * that the same row will be locked repeatedly.
   * @param row the row to unlock
   * @throws RuntimeException if the row is not locked
   */
  public void unlock(Row row) {
    RowLock lock = this.locks.get(row);
    if (lock == null) {
      throw new RuntimeException("Attempted to unlock invalid row lock");
    }
    lock.unlock();
  }

  /**
   * Release the lock for a row and remove the lock from the lock table. This may only be called if the
   * caller has a valid lock for the row. Removing the lock from the table invalidates the lock, and
   * other threads that receive this lock must check its validity. This method should be used if there
   * are many rows, to keep the memory footprint small, or rows are not locked frequently.
   * @param row the row to unlock
   */
  public void unlockAndRemove(Row row) {
    RowLock lock = this.locks.get(row);
    if (lock == null) {
      throw new RuntimeException("Attempted to unlock invalid row lock");
    }
    this.locks.remove(row);
    lock.invalidate();
    lock.unlock();
  }

  /**
   * Delete a (and invalidate) the locks for a single row.
   * @param row the row to delete
   */
  public void removeLock(Row row) {
    Preconditions.checkNotNull(row, "row cannot be null");
    RowLock lock = locks.get(row);
    if (lock != null) {
      lock.invalidate();
      locks.remove(row);
    }
  }

  /**
   * Delete a (and invalidate) the locks for a consecutive range of rows.
   * @param start the first row to delete
   * @param stop the first row not to delete. If null, delete all rows greater than start.
   */
  public void removeRange(Row start, Row stop) {
    Preconditions.checkNotNull(start, "start row cannot be null");
    Map<Row, RowLock> submap = stop == null ? locks.tailMap(start) : locks.subMap(start, stop);
    for (RowLock lock : submap.values()) {
      lock.invalidate();
    }
    submap.clear();
  }

  /**
   * A lock for a row. The lock must be checked for its validity by who ever receives it back from a lock table.
   */
  public static class RowLock {

    private static final Random lockIdGenerator = new Random();

    private final Row row;
    private final long id;
    private final AtomicBoolean locked;
    private boolean valid;

    private final int hash;

    private  RowLock(Row row) {
      this.row = row;
      this.id = lockIdGenerator.nextLong();
      this.locked = new AtomicBoolean(false);
      this.valid = true;
      this.hash = Bytes.hashCode(this.row.value);
    }

    private boolean unlock() {
      synchronized (this.locked) {
        boolean ret = this.locked.compareAndSet(true, false);
        this.locked.notifyAll();
        return ret;
      }
    }

    private void lock() {
      synchronized (this.locked) {
        while (this.locked.get()) {
          try {
            this.locked.wait();
          } catch (InterruptedException e) {
            System.out.println("RowLock.lock() interrupted");
          }
        }
        this.locked.compareAndSet(false, true);
      }
    }

    /**
     * This may only be called if you currently hold the lock.
     */
    private void invalidate() {
      this.valid = false;
    }

    /**
     * This must be called every time the lock was acquired. If it returns false, then
     * the lock (and the row that it locks) have expired and you have to start over.
     */
    public boolean isValid() {
      return valid;
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof RowLock) && this.id == ((RowLock) o).id
        && Bytes.equals(this.row.value, ((RowLock) o).row.value);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("row", this.row)
        .add("id", this.id).add("locked", this.locked).toString();
    }
  }

  /**
   * Represents a row in a lock table.
   */
  public static class Row implements Comparable<Row> {
    private final byte[] value;
    private final int hash;

    public Row(final byte[] value) {
      this.value = value;
      this.hash = value == null ? 0 : Bytes.hashCode(this.value);
    }

    public byte[] getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      return (o instanceof Row) && (this == o || Bytes.equals(this.value, ((Row) o).value));
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public int compareTo(Row r) {
      return Bytes.compareTo(this.value, r.value);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("rowkey", Bytes.toString(this.value)).toString();
    }
  }
}
