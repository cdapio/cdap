package com.continuuity.explore.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.hive.context.CConfCodec;
import com.continuuity.hive.context.ConfigurationUtil;
import com.continuuity.hive.context.ContextManager;
import com.continuuity.hive.context.HConfCodec;
import com.continuuity.hive.context.TxnCodec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.TColumnDesc;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TPrimitiveTypeEntry;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TTableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.hive.service.cli.thrift.TCLIServiceConstants.TYPE_NAMES;

/**
 * Hive implementation of {@link ExploreService}.
 */
public class HiveExploreService extends AbstractIdleService implements ExploreService {
  private static final Logger LOG = LoggerFactory.getLogger(HiveExploreService.class);

  private final CLIService cliService;
  private final CConfiguration cConf;
  private final Configuration hConf;

  // TODO: timeout operations
  private final ConcurrentMap<Handle, OperationInfo> handleMap = Maps.newConcurrentMap();

  @Inject
  public HiveExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                            CConfiguration cConf, Configuration hConf) {
    ContextManager.initialize(txClient, datasetFramework);
    this.cliService = new CLIService();
    this.cConf = cConf;
    this.hConf = hConf;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}...", HiveExploreService.class.getSimpleName());
    HiveConf hiveConf = new HiveConf();
    cliService.init(hiveConf);
    cliService.start();

    TimeUnit.SECONDS.sleep(5);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}...", HiveExploreService.class.getSimpleName());
    cliService.stop();
  }

  @Override
  public Handle execute(String statement) throws ExploreException {
    try {
      Handle handle = Handle.generate();
      LOG.debug("Executing statement: {} with handle {}", statement, handle);

      Map<String, String> sessionConf = getSessionConf(handle);
      SessionHandle sessionHandle = cliService.openSession("hive", "", sessionConf);
      OperationHandle operationHandle = cliService.executeStatementAsync(sessionHandle, statement,
                                                                         ImmutableMap.<String, String>of());
      handleMap.put(handle, new OperationInfo(sessionHandle, operationHandle, sessionConf));
      return handle;
    } catch (Exception e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public Status getStatus(Handle handle) throws ExploreException {
    try {
      OperationHandle operationHandle = getOperationHandle(handle);
      Status status = HiveCompat.getStatus(cliService, operationHandle);
      LOG.debug("Status of handle {} is {}", handle, status);
      return status;
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public List<ColumnDesc> getResultSchema(Handle handle) throws ExploreException {
    try {
      LOG.debug("Getting schema for handle {}", handle);
      ImmutableList.Builder<ColumnDesc> listBuilder = ImmutableList.builder();
      OperationHandle operationHandle = getOperationHandle(handle);
      if (operationHandle.hasResultSet()) {
        TTableSchema tableSchema = cliService.getResultSetMetadata(operationHandle).toTTableSchema();
        for (TColumnDesc tColumnDesc : tableSchema.getColumns()) {
          listBuilder.add(new ColumnDesc(tColumnDesc.getColumnName(), getType(tColumnDesc),
                                         tColumnDesc.getPosition(), tColumnDesc.getComment()));
        }
      }
      return listBuilder.build();
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public List<Row> nextResults(Handle handle, int size) throws ExploreException {
    try {
      LOG.debug("Getting results for handle {}", handle);
      OperationHandle operationHandle = getOperationHandle(handle);
      if (operationHandle.hasResultSet()) {
        // Rowset is an interface in Hive 13, but a class in Hive 12, so we use reflection
        // so that the compiler does not make assumption on the return type of fetchResults
        Object rowSet = cliService.fetchResults(operationHandle, FetchOrientation.FETCH_NEXT, size);
        Class rowSetClass = Class.forName("org.apache.hive.service.cli.RowSet");
        Method toTRowSetMethod = rowSetClass.getMethod("toTRowSet");
        TRowSet tRowSet = (TRowSet) toTRowSetMethod.invoke(rowSet);

        ImmutableList.Builder<Row> rowsBuilder = ImmutableList.builder();
        for (TRow tRow : tRowSet.getRows()) {
          ImmutableList.Builder<Object> colsBuilder = ImmutableList.builder();
          for (TColumnValue tColumnValue : tRow.getColVals()) {
            colsBuilder.add(columnToObject(tColumnValue));
          }
          rowsBuilder.add(new Row(colsBuilder.build()));
        }
        return rowsBuilder.build();
      } else {
        return Collections.emptyList();
      }
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Status cancel(Handle handle) throws ExploreException {
    try {
      LOG.debug("Cancelling operation {}", handle);
      cliService.cancelOperation(getOperationHandle(handle));
      Status status = getStatus(handle);
      if (status.getState() == Status.State.CANCELED) {
        cliService.closeOperation(getOperationHandle(handle));
      }
      return status;
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    } finally {
      cleanUp(handle);
    }
  }

  @Override
  public void close(Handle handle) throws ExploreException {
    try {
      LOG.debug("Closing operation {}", handle);
      cliService.closeOperation(getOperationHandle(handle));
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    } finally {
      cleanUp(handle);
    }
  }

  private void cleanUp(Handle handle) {
    try {
      closeSession(handle);
    } finally {
      try {
        closeTransaction(handle);
      } finally {
        handleMap.remove(handle);
      }
    }
  }

  private void closeSession(Handle handle) {
    try {
      OperationInfo opInfo = handleMap.get(handle);
      if (opInfo == null) {
        throw new ExploreException("Invalid handle provided");
      }
      SessionHandle sessionHandle = opInfo.getSessionHandle();
      cliService.closeSession(sessionHandle);
    } catch (Throwable e) {
      LOG.error("Got error closing session", e);
    }
  }

  private void closeTransaction(Handle handle) {
    try {
      OperationInfo opInfo = handleMap.get(handle);
      Transaction tx = ConfigurationUtil.get(opInfo.getSessionConf(),
                                             Constants.Explore.TX_QUERY_CODEC_KEY,
                                             TxnCodec.INSTANCE);
      LOG.debug("Closing transaction {} for handle {}", tx, handle);

      TransactionSystemClient txClient = ContextManager.getTxClient(new HiveConf());
      // Transaction doesn't involve any changes
      if (txClient.canCommit(tx, ImmutableList.<byte[]>of())) {
        if (!txClient.commit(tx)) {
          txClient.abort(tx);
          LOG.info("Aborting transaction: {}", tx);
        }
      } else {
        // Very unlikely with empty changes
        txClient.invalidate(tx.getWritePointer());
        LOG.info("Invalidating transaction: {}", tx);
      }
    } catch (Throwable e) {
      LOG.error("Got exception while closing transaction.", e);
    }
  }

  private Map<String, String> getSessionConf(Handle handle) throws IOException {
    HiveConf hiveConf = new HiveConf();
    Map<String, String> sessionConf = Maps.newHashMap();
    TransactionSystemClient txClient = ContextManager.getTxClient(hiveConf);
    Transaction tx = txClient.startLong();

    LOG.debug("Transaction {} started for handle {}", tx, handle);
    ConfigurationUtil.set(sessionConf, Constants.Explore.TX_QUERY_CODEC_KEY, TxnCodec.INSTANCE, tx);
    ConfigurationUtil.set(sessionConf, Constants.Explore.CCONF_CODEC_KEY, CConfCodec.INSTANCE, cConf);
    ConfigurationUtil.set(sessionConf, Constants.Explore.HCONF_CODEC_KEY, HConfCodec.INSTANCE, hConf);
    return sessionConf;
  }

  private OperationHandle getOperationHandle(Handle handle) throws ExploreException {
    OperationInfo opInfo = handleMap.get(handle);
    if (opInfo == null) {
      throw new ExploreException("Invalid handle provided");
    }
    return opInfo.getOperationHandle();
  }
  
  private Object columnToObject(TColumnValue tColumnValue) throws ExploreException {
    Object obj;
    if (tColumnValue.isSetBoolVal()) {
      obj = tColumnValue.getBoolVal().isValue();
    } else if (tColumnValue.isSetByteVal()) {
      obj = tColumnValue.getByteVal().getValue();
    } else if (tColumnValue.isSetDoubleVal()) {
      obj = tColumnValue.getDoubleVal().getValue();
    } else if (tColumnValue.isSetI16Val()) {
      obj = tColumnValue.getI16Val().getValue();
    } else if (tColumnValue.isSetI32Val()) {
      obj = tColumnValue.getI32Val().getValue();
    } else if (tColumnValue.isSetI64Val()) {
      obj = tColumnValue.getI64Val().getValue();
    } else if (tColumnValue.isSetStringVal()) {
      obj = tColumnValue.getStringVal().getValue();
    } else {
      throw new ExploreException("Unknown column value encountered: " + tColumnValue);
    }
    return obj;
  }

  private String getType(TColumnDesc tColumnDesc) {
    TPrimitiveTypeEntry primitiveTypeEntry =
      tColumnDesc.getTypeDesc().getTypes().get(0).getPrimitiveEntry();
    return TYPE_NAMES.get(primitiveTypeEntry.getType());
  }

  private static class OperationInfo {
    private final SessionHandle sessionHandle;
    private final OperationHandle operationHandle;
    private final Map<String, String> sessionConf;

    private OperationInfo(SessionHandle sessionHandle, OperationHandle operationHandle,
                          Map<String, String> sessionConf) {
      this.sessionHandle = sessionHandle;
      this.operationHandle = operationHandle;
      this.sessionConf = sessionConf;
    }

    public SessionHandle getSessionHandle() {
      return sessionHandle;
    }

    public OperationHandle getOperationHandle() {
      return operationHandle;
    }

    public Map<String, String> getSessionConf() {
      return sessionConf;
    }
  }
}
