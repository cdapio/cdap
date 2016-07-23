/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.explore.utils.ExploreTableNaming;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.service.cli.CLIService;
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

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Hive 12 implementation of {@link co.cask.cdap.explore.service.ExploreService}.
 * There are 2 changes compared to Hive 13 implementation -
 * <ol>
 *   <li>{@link CLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)} return type has
 *   changed</li>
 *   <li>{@link CLIService#fetchResults(org.apache.hive.service.cli.OperationHandle)} return type has changed</li>
 * </ol>
 */
@SuppressWarnings("UnusedDeclaration")
public class Hive12ExploreService extends BaseHiveExploreService {
  private static final Logger LOG = LoggerFactory.getLogger(Hive12ExploreService.class);

  @Inject
  public Hive12ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                              CConfiguration cConf, Configuration hConf,
                              @Named(Constants.Explore.PREVIEWS_DIR_NAME) File previewsDir,
                              StreamAdmin streamAdmin, NamespaceQueryAdmin namespaceQueryAdmin,
                              SystemDatasetInstantiatorFactory datasetInstantiatorFactory,
                              ExploreTableNaming tableNaming) {
    super(txClient, datasetFramework, cConf, hConf, previewsDir, streamAdmin, namespaceQueryAdmin,
          datasetInstantiatorFactory, tableNaming);
  }

  @Override
  protected CLIService createCLIService() {
    try {
      return CLIService.class.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate CLIService", e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<QueryResult> doFetchNextResults(OperationHandle handle, FetchOrientation fetchOrientation,
                                                 int size) throws Exception {
    Class cliServiceClass = Class.forName("org.apache.hive.service.cli.CLIService");
    Method fetchResultsMethod = cliServiceClass.getMethod("fetchResults", OperationHandle.class,
                                                          FetchOrientation.class, Long.TYPE);
    Object rowSet = fetchResultsMethod.invoke(getCliService(), handle, fetchOrientation, size);

    ImmutableList.Builder<QueryResult> rowsBuilder = ImmutableList.builder();
    Class rowSetClass = Class.forName("org.apache.hive.service.cli.RowSet");
    Method toTRowSetMethod = rowSetClass.getMethod("toTRowSet");
    TRowSet tRowSet = (TRowSet) toTRowSetMethod.invoke(rowSet);
    for (TRow tRow : tRowSet.getRows()) {
      List<Object> cols = Lists.newArrayList();
      for (TColumnValue tColumnValue : tRow.getColVals()) {
        cols.add(tColumnToObject(tColumnValue));
      }
      rowsBuilder.add(new QueryResult(cols));
    }
    return rowsBuilder.build();
  }

  @Override
  protected QueryStatus doFetchStatus(OperationHandle operationHandle)
    throws HiveSQLException, ExploreException, HandleNotFoundException {
    try {
      // In Hive 12, CLIService.getOperationStatus returns OperationState.
      // In Hive 13, CLIService.getOperationStatus returns OperationStatus.
      // Since we use Hive 13 for dev, we need the following workaround to get Hive 12 working.

      Class<? extends CLIService> cliServiceClass = getCliService().getClass();
      Method m = cliServiceClass.getMethod("getOperationStatus", OperationHandle.class);
      OperationState operationState = (OperationState) m.invoke(getCliService(), operationHandle);
      return new QueryStatus(QueryStatus.OpStatus.valueOf(operationState.toString()), operationHandle.hasResultSet());
    } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
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

  @Override
  protected SessionHandle doOpenHiveSession(Map<String, String> sessionConf) throws HiveSQLException {
    SessionHandle sessionHandle = super.doOpenHiveSession(sessionConf);

    // HDP Hive 12 only sets SessionState when an operation is executed.
    // The SessionState is not set during an openSession call.
    // Hence execute a dummy statement during openSession, so that SessionState gets set.
    OperationHandle opHandle = null;
    try {
      opHandle = getCliService().executeStatement(sessionHandle, "set cdap_tmp_var=1",
                                                  Collections.<String, String>emptyMap());
    } catch (Throwable t) {
      LOG.error("Error setting temporary variable to set session state", t);
    } finally {
      if (opHandle != null) {
        try {
          getCliService().closeOperation(opHandle);
        } catch (Throwable t) {
          LOG.debug("Caught exception while closing opHandle", t);
        }
      }
    }
    return sessionHandle;
  }
}
