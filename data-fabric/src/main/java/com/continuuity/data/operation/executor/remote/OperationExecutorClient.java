package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.data.operation.executor.remote.stubs.TOperationException;
import com.continuuity.data.operation.executor.remote.stubs.TOperationExecutor;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * This class is a wrapper around the thrift opex client, it takes
 * Operations, converts them into thrift objects, calls the thrift
 * client, and converts the results back to data fabric classes.
 * This class also instruments the thrift calls with metrics.
 */
public class OperationExecutorClient extends ConverterUtils {

  private static final Logger Log = LoggerFactory.getLogger(OperationExecutorClient.class);
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
  TOperationExecutor.Client client;

  /**
   * The metrics collection client.
   */
  CMetrics metrics = new CMetrics(MetricType.System);

  /**
   * helper method to create a metrics helper.
   */
  MetricsHelper newHelper(String method) {
    return new MetricsHelper(this.getClass(), this.metrics, "opex.client", method);
  }

  MetricsHelper newHelper(String method, byte[] scope) {
    MetricsHelper helper = newHelper(method);
    setScope(helper, scope);
    return helper;
  }

  MetricsHelper newHelper(String method, String scope) {
    MetricsHelper helper = newHelper(method);
    setScope(helper, scope);
    return helper;
  }

  void setScope(MetricsHelper helper, byte[] scope) {
    if (scope != null) {
      helper.setScope(scope);
    }
  }

  void setScope(MetricsHelper helper, String scope) {
    if (scope != null) {
      helper.setScope(scope);
    }
  }

  /**
   * Constructor from an existing, connected thrift transport.
   *
   * @param transport the thrift transport layer. It must already be comnnected
   */
  public OperationExecutorClient(TTransport transport) {
    this.transport = transport;
    // thrift protocol layer, we use binary because so does the service
    TProtocol protocol = new TBinaryProtocol(transport);
    // and create a thrift client
    this.client = new TOperationExecutor.Client(protocol);
  }

  /**
   * close this client. may be called multiple times
   */
  public void close() {
    if (this.transport.isOpen()) {
      this.transport.close();
    }
  }

  public com.continuuity.data2.transaction.Transaction startLong() throws OperationException, TException {
    try {
      return unwrap(client.startLong());
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public com.continuuity.data2.transaction.Transaction startShort() throws OperationException, TException {
    try {
      return unwrap(client.startShort());
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public com.continuuity.data2.transaction.Transaction startShort(int timeout) throws OperationException, TException {
    try {
      return unwrap(client.startShortTimeout(timeout));
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public boolean canCommit(com.continuuity.data2.transaction.Transaction tx, Collection<byte[]> changeIds)
    throws OperationException, TException{
    try {
      return client.canCommitTx(wrap(tx), ImmutableSet.copyOf(Iterables.transform(changeIds, BYTES_WRAPPER)));
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public boolean commit(com.continuuity.data2.transaction.Transaction tx) throws OperationException, TException {
    try {
      return client.commitTx(wrap(tx));
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public void abort(com.continuuity.data2.transaction.Transaction tx) throws OperationException, TException {
    try {
      client.abortTx(wrap(tx));
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

  public void invalidate(com.continuuity.data2.transaction.Transaction tx) throws OperationException, TException {
    try {
      client.invalidateTx(wrap(tx));
    } catch (TOperationException te) {
      throw unwrap(te);
    }
  }

}
