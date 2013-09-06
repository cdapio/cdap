/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.distributed;

import com.continuuity.common.rpc.RPCServiceHandler;
import com.continuuity.data.transaction.thrift.TTransaction;
import com.continuuity.data.transaction.thrift.TTransactionService;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public final class DistributedTransactionServiceHandler implements TTransactionService.Iface, RPCServiceHandler {

  private static final Function<ByteBuffer, byte[]> BYTE_BUFFER_TO_ARRAY = new Function<ByteBuffer, byte[]>() {
    @Override
    public byte[] apply(ByteBuffer input) {
      if (input.hasArray()) {
        byte[] array = input.array();
        if (input.remaining() == array.length) {
          return array;
        }
        return Arrays.copyOfRange(array,
                                  input.arrayOffset() + input.position(),
                                  input.arrayOffset() + input.remaining());
      }
      byte[] array = new byte[input.remaining()];
      input.get(array);
      return array;
    }
  };

  private final TransactionSystemClient transactionSystem;

  public DistributedTransactionServiceHandler(TransactionSystemClient transactionSystem) {
    this.transactionSystem = transactionSystem;
  }

  @Override
  public TTransaction startShort() throws TException {
    Transaction transaction = transactionSystem.startShort();
    return new TTransaction(transaction.getReadPointer(),
                            transaction.getWritePointer(),
                            Converters.encodeLongs(transaction.getInvalids()),
                            Converters.encodeLongs(transaction.getInProgress()),
                            transaction.getFirstShortInProgress());
  }


  @Override
  public TTransaction startShortTimeout(int timeout) throws TException {
    Transaction transaction = transactionSystem.startShort(timeout);
    return new TTransaction(transaction.getReadPointer(),
                            transaction.getWritePointer(),
                            Converters.encodeLongs(transaction.getInvalids()),
                            Converters.encodeLongs(transaction.getInProgress()),
                            transaction.getFirstShortInProgress());
  }

  @Override
  public TTransaction startLong() throws TException {
    Transaction transaction = transactionSystem.startLong();
    return new TTransaction(transaction.getReadPointer(),
                            transaction.getWritePointer(),
                            Converters.encodeLongs(transaction.getInvalids()),
                            Converters.encodeLongs(transaction.getInProgress()),
                            transaction.getFirstShortInProgress());
  }

  @Override
  public boolean canCommit(TTransaction tx, List<ByteBuffer> changeIds) throws TException {
    return transactionSystem.canCommit(Converters.convert(tx), ImmutableList.copyOf(
      Iterables.transform(changeIds, BYTE_BUFFER_TO_ARRAY)
    ));
  }


  @Override
  public boolean commit(TTransaction tx) throws TException {
    return transactionSystem.commit(Converters.convert(tx));
  }

  @Override
  public boolean abort(TTransaction tx) throws TException {
    return transactionSystem.abort(Converters.convert(tx));
  }

  @Override
  public void init() throws Exception {
    // No-op
  }

  @Override
  public void destroy() throws Exception {
    // No-op
  }
}
