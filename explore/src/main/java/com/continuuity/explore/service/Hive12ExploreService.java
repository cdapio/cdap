package com.continuuity.explore.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hive.service.cli.thrift.TCLIServiceConstants.TYPE_NAMES;

/**
 * Hive 12 implementation of {@link ExploreService}. There are 2 changes compared to Hive 13 implementation -
 * <ol>
 *   <li>{@link CLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)} return type has
 *   changed</li>
 *   <li>{@link CLIService#fetchResults(org.apache.hive.service.cli.OperationHandle)} return type has changed</li>
 * </ol>
 */
@SuppressWarnings("UnusedDeclaration")
public class Hive12ExploreService extends BaseHiveExploreService {
  private static final Logger LOG = LoggerFactory.getLogger(Hive12ExploreService.class);

  private final CLIService cliService;

  @Inject
  public Hive12ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                              CConfiguration cConf, Configuration hConf) {
    super(txClient, datasetFramework, cConf, hConf);
    this.cliService = new CLIService();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}...", Hive12ExploreService.class.getSimpleName());
    HiveConf hiveConf = new HiveConf();
    cliService.init(hiveConf);
    cliService.start();
    TimeUnit.SECONDS.sleep(5);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}...", Hive12ExploreService.class.getSimpleName());
    cliService.stop();
  }

  @Override
  public Handle execute(String statement) throws ExploreException {
    try {
      Map<String, String> sessionConf = getSessionConf();
      SessionHandle sessionHandle = cliService.openSession("hive", "", sessionConf);
      OperationHandle operationHandle = cliService.executeStatementAsync(sessionHandle, statement,
                                                                         ImmutableMap.<String, String>of());
      Handle handle = saveOperationInfo(operationHandle, sessionHandle, sessionConf);
      LOG.debug("Executing statement: {} with handle {}", statement, handle);
      return handle;
    } catch (Exception e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public Status getStatus(Handle handle) throws ExploreException {
    try {
      OperationHandle operationHandle = getOperationHandle(handle);
      // In Hive 12, CLIService.getOperationStatus returns OperationState.
      // In Hive 13, CLIService.getOperationStatus returns OperationStatus.
      // Since we use Hive 13 for dev, we need the following workaround to get Hive 12 working.
      Object retStatus = cliService.getOperationStatus(operationHandle);
      OperationState operationState = (OperationState) retStatus;
      Status status = new Status(Status.State.valueOf(operationState.toString()), operationHandle.hasResultSet());
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
        RowSet rowSet = cliService.fetchResults(operationHandle, FetchOrientation.FETCH_NEXT, size);
        TRowSet tRowSet = rowSet.toTRowSet();

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
    }
  }

  @Override
  public void cancel(Handle handle) throws ExploreException {
    try {
      LOG.debug("Cancelling operation {}", handle);
      cliService.cancelOperation(getOperationHandle(handle));
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
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
      try {
        closeSession(handle);
      } finally {
        cleanUp(handle);
      }
    }
  }

  private void closeSession(Handle handle) {
    try {
      cliService.closeSession(getSessionHandle(handle));
    } catch (Throwable e) {
      LOG.error("Got error closing session", e);
    }
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
}
