/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.distributed;

import com.continuuity.data.transaction.thrift.TTransaction;
import com.continuuity.data2.transaction.Transaction;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Helper class to encode long ids into bytes.
 *
 * TODO: This class is temporary. Refactoring is pending to use a IdList class encapsulate sort array of longs
 */
public final class Converters {


  private Converters() {
  }

  /**
   * Encode exclude list.
   */
  // TODO: A more efficient encoding.
  public static ByteBuffer encodeLongs(long[] longs) {
    ByteBuffer buffer = ByteBuffer.allocate(Longs.BYTES * longs.length).order(ByteOrder.BIG_ENDIAN);
    for (long l : longs) {
      buffer.putLong(l);
    }
    buffer.flip();
    return buffer;
  }

  public static long[] decodeLongs(ByteBuffer buffer) {
    buffer.order(ByteOrder.BIG_ENDIAN);
    long[] longs = new long[buffer.remaining() / Longs.BYTES];
    for (int i = 0; i < longs.length; i++) {
      longs[i] = buffer.getLong();
    }
    return longs;
  }

  public static Transaction convert(TTransaction tx) {
    return new Transaction(tx.getReadPointer(),
                           tx.getWritePointer(),
                           decodeLongs(tx.bufferForInvalids()),
                           decodeLongs(tx.bufferForInProgress()));
  }

  public static TTransaction convert(Transaction transaction) {
    return new TTransaction(transaction.getReadPointer(),
                            transaction.getWritePointer(),
                            encodeLongs(transaction.getInvalids()),
                            encodeLongs(transaction.getInProgress()));
  }
}
