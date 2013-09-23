package com.continuuity.data.operation.executor.remote;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.TOperationException;
import com.continuuity.data.operation.executor.remote.stubs.TOperationExecutor;
import com.continuuity.data.operation.executor.remote.stubs.TTransaction2;
import com.google.common.collect.Sets;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * The implementation of a thrift service for operation execution.
 * All operations arrive over the wire as Thrift objects. We must
 * convert ("unwrap") them into data fabric operations, execute using
 * the actual operations executor, and send the results back as
 * ("wrapped") Thrift objects.
 * <p/>
 * Why all this conversion (wrap/unwrap), and not define all Operations
 * themselves as Thrift objects?
 * <ul><li>
 * All the non-thrift executors would have to use the Thrift objects
 * </li><li>
 * Thrift's object model is too restrictive: it has only limited inheritance
 * and no overloading
 * </li><li>
 * Thrift objects are bare-bone, all they have are getters, setters, and
 * basic object methods.
 * </li></ul>
 */
public class TOperationExecutorImpl extends ConverterUtils implements TOperationExecutor.Iface {

  private static final Logger Log = LoggerFactory.getLogger(TOperationExecutorImpl.class);

  /**
   * the operation executor to use for all operations.
   */
  private OperationExecutor opex;

  /**
   * metrics client.
   */
  private CMetrics metrics = new CMetrics(MetricType.System);

  /**
   * helper method to create a metrics helper.
   */
  MetricsHelper newHelper(String method) {
    return new MetricsHelper(this.getClass(), this.metrics, "opex.service", method);
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
   * constructor requires the operation executor.
   */
  public TOperationExecutorImpl(OperationExecutor opex) {
    this.opex = opex;
  }

  // Temporary TxDs2 stuff

  @Override
  public TTransaction2 startLong() throws TOperationException, TException {
    try {
      return wrap(this.opex.startLong());
    } catch (OperationException e) {
      throw wrap(e);
    }
  }

  @Override
  public TTransaction2 startShort() throws TOperationException, TException {
    try {
      return wrap(this.opex.startShort());
    } catch (OperationException e) {
      throw wrap(e);
    }
  }

  @Override
  public TTransaction2 startShortTimeout(int timeout) throws TOperationException, TException {
    try {
      return wrap(this.opex.startShort(timeout));
    } catch (OperationException e) {
      throw wrap(e);
    }
  }


  @Override
  public boolean canCommitTx(TTransaction2 tx, Set<ByteBuffer> changes) throws TOperationException, TException {
    Set<byte[]> changeIds = Sets.newHashSet();
    for (ByteBuffer bb : changes) {
      changeIds.add(bb.array());
    }
    try {
      return this.opex.canCommit(unwrap(tx), changeIds);
    } catch (OperationException e) {
      throw wrap(e);
    }
  }

  @Override
  public boolean commitTx(TTransaction2 tx) throws TOperationException, TException {
    try {
      return this.opex.commit(unwrap(tx));
    } catch (OperationException e) {
      throw wrap(e);
    }
  }

  @Override
  public void abortTx(TTransaction2 tx) throws TOperationException, TException {
    try {
      this.opex.abort(unwrap(tx));
    } catch (OperationException e) {
      throw wrap(e);
    }
  }

  @Override
  public void invalidateTx(TTransaction2 tx) throws TOperationException, TException {
    try {
      this.opex.invalidate(unwrap(tx));
    } catch (OperationException e) {
      throw wrap(e);
    }
  }
}
