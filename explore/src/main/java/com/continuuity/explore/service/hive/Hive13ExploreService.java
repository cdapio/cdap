package com.continuuity.explore.service.hive;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Result;
import com.continuuity.explore.service.Status;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Hive 13 implementation of {@link com.continuuity.explore.service.ExploreService}.
 */
public class Hive13ExploreService extends BaseHiveExploreService {
  private static final Logger LOG = LoggerFactory.getLogger(Hive13ExploreService.class);

  @Inject
  public Hive13ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                              CConfiguration cConf, Configuration hConf, HiveConf hiveConf) {
    super(txClient, datasetFramework, cConf, hConf, hiveConf);
  }

  @Override
  public Status getStatus(Handle handle) throws ExploreException, HandleNotFoundException {
    try {
      OperationHandle operationHandle = getOperationHandle(handle);
      OperationStatus operationStatus = getCliService().getOperationStatus(operationHandle);
      Status status = new Status(Status.OpStatus.valueOf(operationStatus.getState().toString()),
                                 operationHandle.hasResultSet());
      LOG.trace("Status of handle {} is {}", handle, status);
      return status;
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    }
  }

  @Override
  public List<Result> nextResults(Handle handle, int size) throws ExploreException, HandleNotFoundException {
    try {
      LOG.trace("Getting results for handle {}", handle);
      OperationHandle operationHandle = getOperationHandle(handle);
      if (operationHandle.hasResultSet()) {
        RowSet rowSet = getCliService().fetchResults(operationHandle, FetchOrientation.FETCH_NEXT, size);
        ImmutableList.Builder<Result> rowsBuilder = ImmutableList.builder();
        for (Object[] objects : rowSet) {
          rowsBuilder.add(new Result(Lists.newArrayList(objects)));
        }
        return rowsBuilder.build();
      } else {
        return Collections.emptyList();
      }
    } catch (HiveSQLException e) {
      throw new ExploreException(e);
    }
  }
}
