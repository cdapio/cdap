/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.explore.service.hive;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;

import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 * Hive 13 implementation of {@link co.cask.cdap.explore.service.ExploreService}.
 */
public class Hive13ExploreService extends BaseHiveExploreService {

  @Inject
  public Hive13ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                              CConfiguration cConf, Configuration hConf, HiveConf hiveConf,
                              @Named(Constants.Explore.PREVIEWS_DIR_NAME) File previewsDir) {
    super(txClient, datasetFramework, cConf, hConf, hiveConf, previewsDir);
    // This config sets the time Hive CLI getOperationStatus method will wait for the status of
    // a running query.
    System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT.toString(), "50");
  }

  @Override
  protected QueryStatus fetchStatus(OperationHandle operationHandle)
    throws HiveSQLException, ExploreException, HandleNotFoundException {
    OperationStatus operationStatus = getCliService().getOperationStatus(operationHandle);
    return new QueryStatus(QueryStatus.OpStatus.valueOf(operationStatus.getState().toString()),
                           operationHandle.hasResultSet());
  }

  @Override
  protected List<QueryResult> fetchNextResults(OperationHandle operationHandle, int size)
    throws HiveSQLException, ExploreException, HandleNotFoundException {

    if (operationHandle.hasResultSet()) {
      RowSet rowSet = getCliService().fetchResults(operationHandle, FetchOrientation.FETCH_NEXT, size);
      ImmutableList.Builder<QueryResult> rowsBuilder = ImmutableList.builder();
      for (Object[] objects : rowSet) {
        List<QueryResult.ResultObject> cols = Lists.newArrayList();
        for (Object obj : objects) {
          cols.add(rowObjectToQueryResultObject(obj));
        }
        rowsBuilder.add(new QueryResult(cols));
      }
      return rowsBuilder.build();
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  protected OperationHandle doExecute(SessionHandle sessionHandle, String statement)
    throws HiveSQLException, ExploreException {
    return getCliService().executeStatementAsync(sessionHandle, statement, ImmutableMap.<String, String>of());
  }

  protected QueryResult.ResultObject rowObjectToQueryResultObject(Object obj) throws ExploreException {
    if (obj instanceof Boolean) {
      return QueryResult.ResultObject.of((Boolean) obj);
    } else if (obj instanceof Byte) {
      return QueryResult.ResultObject.of((Byte) obj);
    } else if (obj instanceof Double) {
      return QueryResult.ResultObject.of((Double) obj);
    } else if (obj instanceof Short) {
      return QueryResult.ResultObject.of((Short) obj);
    } else if (obj instanceof Integer) {
      return QueryResult.ResultObject.of((Integer) obj);
    } else if (obj instanceof Long) {
      return QueryResult.ResultObject.of((Long) obj);
    } else if (obj instanceof String) {
      return QueryResult.ResultObject.of((String) obj);
    } else if (obj instanceof byte[]) {
      return QueryResult.ResultObject.of((byte[]) obj);
    } else if (obj == null) {
      return QueryResult.ResultObject.of();
    }
    throw new ExploreException("Unknown column value encountered: " + obj);
  }
}
