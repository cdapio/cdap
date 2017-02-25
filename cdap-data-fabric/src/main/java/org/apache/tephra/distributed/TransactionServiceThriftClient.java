/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.distributed;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.tephra.InvalidTruncateTimeException;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionCouldNotTakeSnapshotException;
import org.apache.tephra.TransactionNotInProgressException;
import org.apache.tephra.distributed.thrift.TGenericException;
import org.apache.tephra.distributed.thrift.TInvalidTruncateTimeException;
import org.apache.tephra.distributed.thrift.TTransactionCouldNotTakeSnapshotException;
import org.apache.tephra.distributed.thrift.TTransactionNotInProgressException;
import org.apache.tephra.distributed.thrift.TTransactionServer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is a wrapper around the thrift tx service client, it takes
 * Operations, converts them into thrift objects, calls the thrift
 * client, and converts the results back to data fabric classes.
 * This class also instruments the thrift calls with metrics.
 */
public class TransactionServiceThriftClient {
  private static final Function<byte[], ByteBuffer> BYTES_WRAPPER = new Function<byte[], ByteBuffer>() {
    @Override
    public ByteBuffer apply(byte[] input) {
      return ByteBuffer.wrap(input);
    }
  };

  private static final Logger LOG = LoggerFactory.getLogger(TransactionServiceThriftClient.class);

  /**
   * The thrift transport layer. We need this when we close the connection.
   */
  TTransport transport;

  /**
   * The actual thrift client.
   */
  TTransactionServer.Client client;

  /**
   * Whether this client is valid for use.
   */
  private final AtomicBoolean isValid = new AtomicBoolean(true);

  /**
   * Constructor from an existing, connected thrift transport.
   *
   * @param transport the thrift transport layer. It must already be connected
   */
  public TransactionServiceThriftClient(TTransport transport) {
    this.transport = transport;
    // thrift protocol layer, we use binary because so does the service
    TProtocol protocol = new TBinaryProtocol(transport);
    // and create a thrift client
    this.client = new TTransactionServer.Client(protocol);
  }

  /**
   * close this client. may be called multiple times
   */
  public void close() {
    if (this.transport.isOpen()) {
      this.transport.close();
    }
  }

  public Transaction startLong() throws TException {
    try {
      return TransactionConverterUtils.unwrap(client.startLong());
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public Transaction startShort() throws TException {
    try {
      return TransactionConverterUtils.unwrap(client.startShort());
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public Transaction startShort(int timeout) throws TException {
    try {
      return TransactionConverterUtils.unwrap(client.startShortWithTimeout(timeout));
    } catch (TGenericException e) {
      // currently, we only expect IllegalArgumentException here, if the timeout is invalid
      if (!IllegalArgumentException.class.getName().equals(e.getOriginalExceptionClass())) {
        LOG.trace("Expecting only {} as the original exception class but found {}",
                  IllegalArgumentException.class.getName(), e.getOriginalExceptionClass());
        throw e;
      }
      throw new IllegalArgumentException(e.getMessage());
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public boolean canCommit(Transaction tx, Collection<byte[]> changeIds)
    throws TException, TransactionNotInProgressException {
    try {
      return client.canCommitTx(TransactionConverterUtils.wrap(tx),
                                ImmutableSet.copyOf(Iterables.transform(changeIds, BYTES_WRAPPER))).isValue();
    } catch (TTransactionNotInProgressException e) {
      throw new TransactionNotInProgressException(e.getMessage());
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }



  public boolean commit(Transaction tx) throws TException, TransactionNotInProgressException {
    try {
      return client.commitTx(TransactionConverterUtils.wrap(tx)).isValue();
    } catch (TTransactionNotInProgressException e) {
      throw new TransactionNotInProgressException(e.getMessage());
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public void abort(Transaction tx) throws TException {
    try {
      client.abortTx(TransactionConverterUtils.wrap(tx));
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public boolean invalidate(long tx) throws TException {
    try {
      return client.invalidateTx(tx);
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public Transaction checkpoint(Transaction tx) throws TException {
    try {
      return TransactionConverterUtils.unwrap(client.checkpoint(TransactionConverterUtils.wrap(tx)));
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public InputStream getSnapshotStream() throws TException, TransactionCouldNotTakeSnapshotException {
    try {
      ByteBuffer buffer = client.getSnapshot();
      if (buffer.hasArray()) {
        return new ByteArrayInputStream(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
      }

      // The ByteBuffer is not backed by array. Read the content to a new byte array and return an InputStream of that.
      byte[] snapshot = new byte[buffer.remaining()];
      buffer.get(snapshot);
      return new ByteArrayInputStream(snapshot);
    } catch (TTransactionCouldNotTakeSnapshotException e) {
      throw new TransactionCouldNotTakeSnapshotException(e.getMessage());
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public String status() throws TException {
    try {
      return client.status();
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public void resetState() throws TException {
    try {
        client.resetState();
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public boolean truncateInvalidTx(Set<Long> invalidTxIds) throws TException {
    try {
      return client.truncateInvalidTx(invalidTxIds).isValue();
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }
  
  public boolean truncateInvalidTxBefore(long time) throws TException, InvalidTruncateTimeException {
    try {
      return client.truncateInvalidTxBefore(time).isValue();
    } catch (TInvalidTruncateTimeException e) {
      throw new InvalidTruncateTimeException(e.getMessage());
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }
  
  public int getInvalidSize() throws TException {
    try {
      return client.invalidTxSize();
    } catch (TException e) {
      isValid.set(false);
      throw e;
    }
  }

  public boolean isValid() {
    return isValid.get();
  }
}
