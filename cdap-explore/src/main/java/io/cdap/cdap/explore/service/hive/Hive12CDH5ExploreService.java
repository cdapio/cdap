/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.explore.service.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.explore.HiveUtilities;
import io.cdap.cdap.explore.service.ExploreException;
import io.cdap.cdap.explore.service.HandleNotFoundException;
import io.cdap.cdap.proto.QueryResult;
import io.cdap.cdap.proto.QueryStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.tephra.TransactionSystemClient;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Hive 12 patched for CDH5 implementation of {@link io.cdap.cdap.explore.service.ExploreService}.
 * There is only 1 change compared to Hive 13 implementation -
 * <ol>
 *   <li>{@link org.apache.hive.service.cli.CLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)}
 *   return type has changed</li>
 * </ol>
 */
public class Hive12CDH5ExploreService extends BaseHiveExploreService {

  @Inject
  protected Hive12CDH5ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                                     CConfiguration cConf, Configuration hConf,
                                     @Named(Constants.Explore.PREVIEWS_DIR_NAME) File previewsDir,
                                     @Named(Constants.Explore.CREDENTIALS_DIR_NAME) File credentialsDir,
                                     NamespaceQueryAdmin namespaceQueryAdmin,
                                     SystemDatasetInstantiatorFactory datasetInstantiatorFactory) {
    super(txClient, datasetFramework, cConf, hConf, previewsDir, credentialsDir, namespaceQueryAdmin,
          datasetInstantiatorFactory);
  }

  @Override
  protected CLIService createCLIService() {
    try {
      return CLIService.class.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate CLIService", e);
    }
  }

  @Override
  protected List<QueryResult> doFetchNextResults(OperationHandle handle, FetchOrientation fetchOrientation,
                                                 int size) throws Exception {
    Class<?> cliServiceClass = Class.forName("org.apache.hive.service.cli.CLIService");
    Method fetchResultsMethod = cliServiceClass.getMethod(
      "fetchResults", OperationHandle.class, FetchOrientation.class, Long.TYPE);
    Object rowSet = fetchResultsMethod.invoke(getCliService(), handle, fetchOrientation, size);

    ImmutableList.Builder<QueryResult> rowsBuilder = ImmutableList.builder();
    Class<?> rowSetClass = Class.forName("org.apache.hive.service.cli.RowSet");
    Method toTRowSetMethod = rowSetClass.getMethod("toTRowSet");
    TRowSet tRowSet = (TRowSet) toTRowSetMethod.invoke(rowSet);
    for (TRow tRow : tRowSet.getRows()) {
      List<Object> cols = Lists.newArrayList();
      for (TColumnValue tColumnValue : tRow.getColVals()) {
        cols.add(HiveUtilities.tColumnToObject(tColumnValue));
      }
      rowsBuilder.add(new QueryResult(cols));
    }
    return rowsBuilder.build();
  }

  @Override
  protected QueryStatus doFetchStatus(OperationHandle handle)
    throws HiveSQLException, ExploreException, HandleNotFoundException {
    OperationStatus operationStatus = getCliService().getOperationStatus(handle);
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    HiveSQLException hiveExn = operationStatus.getOperationException();
    if (hiveExn != null) {
      return new QueryStatus(hiveExn.getMessage(), hiveExn.getSQLState());
    }
    return new QueryStatus(QueryStatus.OpStatus.valueOf(operationStatus.getState().toString()),
                           handle.hasResultSet());
  }

  @Override
  protected OperationHandle executeSync(SessionHandle sessionHandle, String statement)
    throws HiveSQLException, ExploreException {
    return getCliService().executeStatement(sessionHandle, statement, ImmutableMap.<String, String>of());
  }

  @Override
  protected OperationHandle executeAsync(SessionHandle sessionHandle, String statement)
    throws HiveSQLException, ExploreException {
    return getCliService().executeStatementAsync(sessionHandle, statement, ImmutableMap.<String, String>of());
  }
}
