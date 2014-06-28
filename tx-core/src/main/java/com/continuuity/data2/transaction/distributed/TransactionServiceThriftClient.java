package com.continuuity.data2.transaction.distributed;

import com.continuuity.data2.transaction.distributed.thrift.TTransactionCouldNotTakeSnapshotException;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionNotInProgressException;
import com.continuuity.data2.transaction.distributed.thrift.TTransactionServer;
import com.continuuity.internal.io.ByteBufferInputStream;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;

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

  /**
   * The thrift transport layer. We need this when we close the connection.
   */
  TTransport transport;

  /**
   * The actual thrift client.
   */
  TTransactionServer.Client client;

  /**
   * Constructor from an existing, connected thrift transport.
   *
   * @param transport the thrift transport layer. It must already be comnnected
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

  public com.continuuity.data2.transaction.Transaction startLong() throws TException {
    return ConverterUtils.unwrap(client.startLong());
  }

  public com.continuuity.data2.transaction.Transaction startShort() throws TException {
      return ConverterUtils.unwrap(client.startShort());
  }

  public com.continuuity.data2.transaction.Transaction startShort(int timeout) throws TException {
      return ConverterUtils.unwrap(client.startShortTimeout(timeout));
  }

  public boolean canCommit(com.continuuity.data2.transaction.Transaction tx, Collection<byte[]> changeIds)
    throws TTransactionNotInProgressException, TException {

      return client.canCommitTx(ConverterUtils.wrap(tx),
                                ImmutableSet.copyOf(Iterables.transform(changeIds, BYTES_WRAPPER))).isValue();
  }

  public boolean commit(com.continuuity.data2.transaction.Transaction tx)
    throws TTransactionNotInProgressException, TException {

      return client.commitTx(ConverterUtils.wrap(tx)).isValue();
  }

  public void abort(com.continuuity.data2.transaction.Transaction tx) throws TException {
      client.abortTx(ConverterUtils.wrap(tx));
  }

  public boolean invalidate(long tx) throws TException {
    return client.invalidateTx(tx);
  }

  public InputStream getSnapshotStream() throws TTransactionCouldNotTakeSnapshotException, TException {
    ByteBuffer buffer = client.getSnapshot();
    return new ByteBufferInputStream(buffer);
  }

  public String status() throws TException { return client.status(); }

  public void resetState() throws TException {
    client.resetState();
  }
}
