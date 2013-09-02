/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.distributed;

import com.continuuity.data.transaction.thrift.TTransactionService;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 *
 */
public final class DistributedTransactionClient implements TransactionSystemClient {

  private static final Function<byte[], ByteBuffer> BYTE_ARRAY_TO_BUFFER = new Function<byte[], ByteBuffer>() {
    @Override
    public ByteBuffer apply(byte[] input) {
      return ByteBuffer.wrap(input);
    }
  };

  private final TTransactionService.Client rpcClient;

  public DistributedTransactionClient(TTransactionService.Client rpcClient) {
    this.rpcClient = rpcClient;
  }

  @Override
  public Transaction start() {
    try {
      return Converters.convert(rpcClient.start());
    } catch (TException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Transaction start(Integer timeout) {
    // todo implement this when we replace opex with a tx manager service.
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds) {
    try {
      return rpcClient.canCommit(Converters.convert(tx), ImmutableList.copyOf(
        Iterables.transform(changeIds, BYTE_ARRAY_TO_BUFFER))
      );
    } catch (TException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean commit(Transaction tx) {
    try {
      return rpcClient.commit(Converters.convert(tx));
    } catch (TException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean abort(Transaction tx) {
    try {
      return rpcClient.abort(Converters.convert(tx));
    } catch (TException e) {
      throw Throwables.propagate(e);
    }
  }
}
