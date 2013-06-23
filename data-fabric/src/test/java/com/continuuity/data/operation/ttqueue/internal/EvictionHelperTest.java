package com.continuuity.data.operation.ttqueue.internal;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.memory.MemoryReadPointer;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.continuuity.data.operation.ttqueue.internal.TTQueueNewConstants.INVALID_ENTRY_ID;

public class EvictionHelperTest {

  @Test
  public void testGetMinEvictionEntry() {
    testGetMinEvictionEntry(new EvictionHelper());
  }

  @Test
  public void testGetMinEvictionEntryCleanup() {
    testGetMinEvictionEntry(new CleanupEvictionHelper());
  }

  private void testGetMinEvictionEntry(EvictionHelper evictionHelper) {
    long txnPtr1 = 10000;
    long txnPtr2 = 10001;
    long txnPtr3 = 10002;

    evictionHelper.addMinAckEntry(txnPtr1, 10);
    evictionHelper.addMinAckEntry(txnPtr2, 11);
    evictionHelper.addMinAckEntry(txnPtr3, 13);

    // None of the transactions are committed
    MemoryReadPointer memoryReadPointer = new MemoryReadPointer(9999);
    Assert.assertEquals(9, evictionHelper.getMinEvictionEntry(14, memoryReadPointer));
    // There is no good way to determine getMinEvictionEntry when minUnacked entry is less than minUncommitted entry
    Assert.assertEquals(INVALID_ENTRY_ID, evictionHelper.getMinEvictionEntry(7, memoryReadPointer));

    // Commit txn 1
    memoryReadPointer = new MemoryReadPointer(txnPtr1);
    Assert.assertEquals(10, evictionHelper.getMinEvictionEntry(14, memoryReadPointer));
    // There is no good way to determine getMinEvictionEntry when minUnacked entry is less than minUncommitted entry
    Assert.assertEquals(INVALID_ENTRY_ID, evictionHelper.getMinEvictionEntry(7, memoryReadPointer));

    // Commit only txn 2
    memoryReadPointer = new MemoryReadPointer(txnPtr2, Sets.newHashSet(txnPtr1));
    Assert.assertEquals(9, evictionHelper.getMinEvictionEntry(14, memoryReadPointer));
    Assert.assertEquals(9, evictionHelper.getMinEvictionEntry(12, memoryReadPointer));
    Assert.assertEquals(INVALID_ENTRY_ID, evictionHelper.getMinEvictionEntry(7, memoryReadPointer));
    // Commit both txns 1 and 2
    memoryReadPointer = new MemoryReadPointer(txnPtr2);
    Assert.assertEquals(12, evictionHelper.getMinEvictionEntry(14, memoryReadPointer));
    Assert.assertEquals(11, evictionHelper.getMinEvictionEntry(12, memoryReadPointer));
    Assert.assertEquals(INVALID_ENTRY_ID, evictionHelper.getMinEvictionEntry(7, memoryReadPointer));

    // Commit only txn 3
    memoryReadPointer = new MemoryReadPointer(txnPtr3, Sets.newHashSet(txnPtr1, txnPtr2));
    Assert.assertEquals(9, evictionHelper.getMinEvictionEntry(14, memoryReadPointer));
    Assert.assertEquals(9, evictionHelper.getMinEvictionEntry(12, memoryReadPointer));
    Assert.assertEquals(INVALID_ENTRY_ID, evictionHelper.getMinEvictionEntry(7, memoryReadPointer));
    // Commit only txns 2 and 3
    memoryReadPointer = new MemoryReadPointer(txnPtr3, Sets.newHashSet(txnPtr1));
    Assert.assertEquals(9, evictionHelper.getMinEvictionEntry(14, memoryReadPointer));
    Assert.assertEquals(9, evictionHelper.getMinEvictionEntry(12, memoryReadPointer));
    Assert.assertEquals(INVALID_ENTRY_ID, evictionHelper.getMinEvictionEntry(7, memoryReadPointer));
    // Commit only txns 1 and 3
    memoryReadPointer = new MemoryReadPointer(txnPtr3, Sets.newHashSet(txnPtr2));
    Assert.assertEquals(10, evictionHelper.getMinEvictionEntry(14, memoryReadPointer));
    Assert.assertEquals(10, evictionHelper.getMinEvictionEntry(12, memoryReadPointer));
    Assert.assertEquals(INVALID_ENTRY_ID, evictionHelper.getMinEvictionEntry(7, memoryReadPointer));
    // Commit txns 1, 2, and 3
    memoryReadPointer = new MemoryReadPointer(txnPtr3);
    Assert.assertEquals(13, evictionHelper.getMinEvictionEntry(14, memoryReadPointer));
    Assert.assertEquals(11, evictionHelper.getMinEvictionEntry(12, memoryReadPointer));
    Assert.assertEquals(INVALID_ENTRY_ID, evictionHelper.getMinEvictionEntry(7, memoryReadPointer));
  }

  static class CleanupEvictionHelper extends EvictionHelper {
    private EvictionHelper evictionHelper = new EvictionHelper();

    @Override
    public void addMinAckEntry(long txnPtr, long minAckEntry) {
      evictionHelper.addMinAckEntry(txnPtr, minAckEntry);
    }

    @Override
    public long getMinEvictionEntry(long minUnackedEntry, ReadPointer readPointer) {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        evictionHelper.encode(new BinaryEncoder(bos));
        long minEntry = evictionHelper.getMinEvictionEntry(minUnackedEntry, readPointer);
        evictionHelper = EvictionHelper.decode(new BinaryDecoder(new ByteArrayInputStream(bos.toByteArray())));
        return minEntry;
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void cleanup(Transaction transaction) {
      evictionHelper.cleanup(transaction);
    }

    @Override
    public int size() {
      return evictionHelper.size();
    }

    @Override
    public String toString() {
      return evictionHelper.toString();
    }

    @Override
    public void encode(Encoder encoder) throws IOException {
      evictionHelper.encode(encoder);
    }

    public static EvictionHelper decode(Decoder decoder) throws IOException {
      return EvictionHelper.decode(decoder);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
      return evictionHelper.equals(o);
    }

    @Override
    public int hashCode() {
      return evictionHelper.hashCode();
    }
  }
}
