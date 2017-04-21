/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.tephra.TransactionSystemClient;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;

/**
 * Hive 14 implementation of {@link co.cask.cdap.explore.service.ExploreService}.
 */
public class Hive14ExploreService extends BaseHiveExploreService {

  // Used to store the getOperationStatus from Hive's CLIService class. The number of arguments for this method
  // varies between versions, so we get this method on startup using reflection and then call the correct version
  // based on the number of argument it takes.
  private Method getOperationStatus;

  @Inject
  public Hive14ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                              CConfiguration cConf, Configuration hConf,
                              @Named(Constants.Explore.PREVIEWS_DIR_NAME) File previewsDir,
                              @Named(Constants.Explore.CREDENTIALS_DIR_NAME) File credentialsDir,
                              StreamAdmin streamAdmin, NamespaceQueryAdmin namespaceQueryAdmin,
                              SystemDatasetInstantiatorFactory datasetInstantiatorFactory,
                              AuthorizationEnforcementService authorizationEnforcementService,
                              AuthorizationEnforcer authorizationEnforcer,
                              AuthenticationContext authenticationContext) {
    super(txClient, datasetFramework, cConf, hConf, previewsDir, credentialsDir, streamAdmin, namespaceQueryAdmin,
          datasetInstantiatorFactory, authorizationEnforcementService, authorizationEnforcer,
          authenticationContext);
    // This config sets the time Hive CLI getOperationStatus method will wait for the status of
    // a running query.
    System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT.toString(), "50");
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
    getOperationStatus = initOperationStatus();
  }

  private Method initOperationStatus() {
    /*
    Hive 1.2.1000.2.6.0.0-598, that ships with HDP 2.6.0 introduced a change as part of
    https://issues.apache.org/jira/browse/HIVE-15473 in the getOperationStatus method and introduced a boolean argument
    to get the progress update for the operation. Previous versions of the method just took an {@link OperationHandle}
    as the single argument.
    To handle this we are using reflection to find out how many arguments the method takes and then call it accordingly.
     */
    CLIService cliService = getCliService();
    Method getOperationStatus = null;
    for (Method method : cliService.getClass().getMethods()) {
      if ("getOperationStatus".equals(method.getName())) {
        getOperationStatus = method;
        break;
      }
    }
    if (getOperationStatus == null) {
      throw new RuntimeException("Unable to find getOperationStatus method from the Hive CLIService.");
    }
    return getOperationStatus;
  }

  @Override
  protected List<QueryResult> doFetchNextResults(OperationHandle handle, FetchOrientation fetchOrientation,
                                                 int size) throws Exception {
    RowSet rowSet = getCliService().fetchResults(handle, fetchOrientation, size, FetchType.QUERY_OUTPUT);
    ImmutableList.Builder<QueryResult> rowsBuilder = ImmutableList.builder();
    for (Object[] row : rowSet) {
      List<Object> cols = Lists.newArrayList(row);
      rowsBuilder.add(new QueryResult(cols));
    }
    return rowsBuilder.build();
  }

  @Override
  protected QueryStatus doFetchStatus(OperationHandle operationHandle)
    throws HiveSQLException, ExploreException, HandleNotFoundException {

    OperationStatus operationStatus;
    CLIService cliService = getCliService();

    // Call the getOperationStatus method based on the number of arguments it expects.
    try {
      if (getOperationStatus.getParameterTypes().length == 2) {
        operationStatus = (OperationStatus) getOperationStatus.invoke(cliService, operationHandle, true);
      } else {
        operationStatus = (OperationStatus) getOperationStatus.invoke(cliService, operationHandle);
      }
    } catch (IndexOutOfBoundsException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Failed to get the status of the operation.", e);
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    HiveSQLException hiveExn = operationStatus.getOperationException();
    if (hiveExn != null) {
      return new QueryStatus(hiveExn.getMessage(), hiveExn.getSQLState());
    }
    return new QueryStatus(QueryStatus.OpStatus.valueOf(operationStatus.getState().toString()),
                           operationHandle.hasResultSet());
  }

  @Override
  protected OperationHandle executeSync(SessionHandle sessionHandle, String statement)
    throws HiveSQLException, ExploreException {
    return getCliService().executeStatement(sessionHandle, statement, new HashMap<String, String>());
  }

  @Override
  protected OperationHandle executeAsync(SessionHandle sessionHandle, String statement)
    throws HiveSQLException, ExploreException {
    return getCliService().executeStatementAsync(sessionHandle, statement, new HashMap<String, String>());
  }
}
