package com.continuuity.explore.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Hive 13 implementation of {@link com.continuuity.explore.service.ExploreService}.
 */
public class Hive13ExploreService extends BaseHiveExploreService {
  private static final Logger LOG = LoggerFactory.getLogger(Hive13ExploreService.class);

  private final CLIService cliService;

  @Inject
  public Hive13ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                              CConfiguration cConf, Configuration hConf) {
    super(txClient, datasetFramework, cConf, hConf);
    this.cliService = new CLIService();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}...", Hive13ExploreService.class.getSimpleName());
    HiveConf hiveConf = new HiveConf();
    cliService.init(hiveConf);
    cliService.start();
    TimeUnit.SECONDS.sleep(5);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}...", Hive13ExploreService.class.getSimpleName());
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
      OperationStatus operationStatus = cliService.getOperationStatus(operationHandle);
      Status status = new Status(Status.State.valueOf(operationStatus.getState().toString()),
                                 operationHandle.hasResultSet());
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
        TableSchema tableSchema = cliService.getResultSetMetadata(operationHandle);
        for (ColumnDescriptor tColumnDesc : tableSchema.getColumnDescriptors()) {
          listBuilder.add(new ColumnDesc(tColumnDesc.getName(), tColumnDesc.getType().getName(),
                                         tColumnDesc.getOrdinalPosition(), tColumnDesc.getComment()));
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
        ImmutableList.Builder<Row> rowsBuilder = ImmutableList.builder();
        for (Object[] objects : rowSet) {
          rowsBuilder.add(new Row(Lists.newArrayList(objects)));
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
      SessionHandle sessionHandle = getSessionHandle(handle);
      cliService.closeSession(sessionHandle);
    } catch (Throwable e) {
      LOG.error("Got error closing session", e);
    }
  }
}
