package com.continuuity.data.engine.memory;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class RowLock {

  private static final Random lockIdGenerator = new Random();

  final Row row;
  final long id;
  final AtomicBoolean locked;

  public RowLock(Row row) {
    this.row = row;
    this.id = lockIdGenerator.nextLong();
    this.locked = new AtomicBoolean(false);
  }

  public boolean unlock() {
    synchronized (this.locked) {
      boolean ret = this.locked.compareAndSet(true, false);
      this.locked.notifyAll();
      return ret;
    }
  }

  public void lock() {
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

  @Override
  public boolean equals(Object o) {
    return (o instanceof RowLock) && this.id == ((RowLock)o).id
      && Bytes.equals(this.row.value, ((RowLock) o).row.value);
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(this.row.value);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("row", this.row)
      .add("id", this.id).add("locked", this.locked).toString();
  }
}