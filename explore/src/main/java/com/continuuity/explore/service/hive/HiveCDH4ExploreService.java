package com.continuuity.explore.service.hive;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Result;
import com.continuuity.explore.service.Status;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Hive patched for CDH4 implementation of {@link com.continuuity.explore.service.ExploreService}.
 */
public class HiveCDH4ExploreService extends BaseHiveExploreService {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCDH4ExploreService.class);

  @Inject
  protected HiveCDH4ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                                   CConfiguration cConf, Configuration hConf, HiveConf hiveConf) {
    super(txClient, datasetFramework, cConf, hConf, hiveConf);
    System.setProperty("hive.server2.blocking.query", "false");
  }

  @Override
  protected Status fetchStatus(OperationHandle operationHandle)
    throws HiveSQLException, ExploreException, HandleNotFoundException {
    try {
      // In Hive 12, CLIService.getOperationStatus returns OperationState.
      // In Hive 13, CLIService.getOperationStatus returns OperationStatus.
      // Since we use Hive 13 for dev, we need the following workaround to get Hive 12 working.

      Class cliServiceClass = getCliService().getClass();
      Method m = cliServiceClass.getMethod("getOperationStatus", OperationHandle.class);
      OperationState operationState = (OperationState) m.invoke(getCliService(), operationHandle);
      return new Status(Status.OpStatus.valueOf(operationState.toString()), operationHandle.hasResultSet());
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e);
    } catch (NoSuchMethodException e) {
      throw  Throwables.propagate(e);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected List<Result> fetchNextResults(OperationHandle operationHandle, int size)
    throws HiveSQLException, ExploreException, HandleNotFoundException {
    try {
      if (operationHandle.hasResultSet()) {
        // Rowset is an interface in Hive 13, but a class in Hive 12, so we use reflection
        // so that the compiler does not make assumption on the return type of fetchResults
        Object rowSet = getCliService().fetchResults(operationHandle, FetchOrientation.FETCH_NEXT, size);
        Class rowSetClass = Class.forName("org.apache.hive.service.cli.RowSet");
        Method toTRowSetMethod = rowSetClass.getMethod("toTRowSet");
        TRowSet tRowSet = (TRowSet) toTRowSetMethod.invoke(rowSet);

        ImmutableList.Builder<Result> rowsBuilder = ImmutableList.builder();
        for (TRow tRow : tRowSet.getRows()) {
          ImmutableList.Builder<Object> colsBuilder = ImmutableList.builder();
          for (TColumnValue tColumnValue : tRow.getColVals()) {
            colsBuilder.add(columnToObject(tColumnValue));
          }
          rowsBuilder.add(new Result(colsBuilder.build()));
        }
        return rowsBuilder.build();
      } else {
        return Collections.emptyList();
      }
    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    } catch (NoSuchMethodException e) {
      throw Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected OperationHandle doExecute(SessionHandle sessionHandle, String statement)
    throws HiveSQLException, ExploreException {
    return getCliService().executeStatement(sessionHandle, statement, ImmutableMap.<String, String>of());
  }

  @Override
  public void cancel(Handle handle) throws ExploreException, HandleNotFoundException, SQLException {
    LOG.warn("Trying to cancel operation with handle {}", handle);
    throw new ExploreException("Cancel operation is not supported with CDH4.");
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
}
