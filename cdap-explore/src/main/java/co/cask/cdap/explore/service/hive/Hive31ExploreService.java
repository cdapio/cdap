/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.*;
import org.apache.tephra.TransactionSystemClient;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive 3.1 implementation of {@link co.cask.cdap.explore.service.ExploreService}.
 */
public class Hive31ExploreService extends BaseHiveExploreService {

  private static final Logger LOG = LoggerFactory.getLogger(Hive31ExploreService.class);

  private Method getOperationStatus;

  @Inject
  public Hive31ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                              CConfiguration cConf, Configuration hConf,
                              @Named(Constants.Explore.PREVIEWS_DIR_NAME) File previewsDir,
                              @Named(Constants.Explore.CREDENTIALS_DIR_NAME) File credentialsDir,
                              StreamAdmin streamAdmin, NamespaceQueryAdmin namespaceQueryAdmin,
                              SystemDatasetInstantiatorFactory datasetInstantiatorFactory,
                              AuthorizationEnforcer authorizationEnforcer,
                              AuthenticationContext authenticationContext) {
    super(txClient, datasetFramework, cConf, hConf, previewsDir, credentialsDir, streamAdmin, namespaceQueryAdmin,
          datasetInstantiatorFactory, authorizationEnforcer,
          authenticationContext);
    
//    LOG.info("Inside Hive31ExploreService constructor:: previewsDir: " + previewsDir
//    		+ ", credentialsDir: " + credentialsDir
//    		);
//    LOG.info("cConf:: " + cConf);
//    LOG.info("cConf:: " + cConf.get(HiveConf.ConfVars.POSTEXECHOOKS.toString()));
//    LOG.info("hConf:: " + hConf);
//    LOG.info("hConf:: " + hConf.get(HiveConf.ConfVars.POSTEXECHOOKS.toString()));
    
    final String post_hooks = hConf.get(HiveConf.ConfVars.POSTEXECHOOKS.toString());
    
    if(post_hooks!=null && post_hooks.contains("org.apache.atlas.hive.hook.HiveHook")) {
    	final String post_hooks_seprator = ",";
    	String[] post_hooks_arr = post_hooks.split(post_hooks_seprator);
    	String seprator = "";
    	StringBuilder sb = new StringBuilder();
    	for (int i = 0; i < post_hooks_arr.length; i++) {
    		if(!"org.apache.atlas.hive.hook.HiveHook".equals(post_hooks_arr[i])) {
    			sb.append(seprator);
    			sb.append(post_hooks_arr[i]);
    		}
    		seprator=post_hooks_seprator;
		}
    	LOG.info("Updating POSTEXECHOOKS to :: " + sb.toString() + " , original value:: " + post_hooks);
    	System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.toString(), sb.toString());
    	hConf.set(HiveConf.ConfVars.POSTEXECHOOKS.toString(), sb.toString(), "co.cask.cdap.explore.service.hive.Hive31ExploreService");
    }

    LOG.info("Inside Hive31ExploreService constructor:: post_hooks: " + post_hooks
    		);    
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();
    getOperationStatus = initOperationStatus();
  }

  private Method initOperationStatus() {
	  LOG.info("Inside Hive31ExploreService.initOperationStatus");
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
	  LOG.info("Inside Hive31ExploreService.doFetchNextResults");
	  
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

	  LOG.info("Inside Hive31ExploreService.doFetchStatus");
	  
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
	  LOG.info("Inside Hive31ExploreService.executeSync, statement:: " + statement + " , sessionHandle: " + sessionHandle);
    return getCliService().executeStatement(sessionHandle, statement, new HashMap<String, String>());
  }

  @Override
  protected OperationHandle executeAsync(SessionHandle sessionHandle, String statement)
    throws HiveSQLException, ExploreException {
	  LOG.info("Inside Hive31ExploreService.executeAsync, statement:: " + statement + " , sessionHandle: " + sessionHandle);
    return getCliService().executeStatementAsync(sessionHandle, statement, new HashMap<String, String>());
  }
  
  @Override
	public QueryHandle execute(NamespaceId namespace, String[] statements) throws ExploreException, SQLException {
	  LOG.info("Inside Hive31ExploreService.execute, statement:: " + Arrays.toString(statements) + " , namespace: " + namespace);
		return super.execute(namespace, statements);
	}
  
  @Override
	public OperationInfo getOperationInfo(QueryHandle queryHandle) throws HandleNotFoundException {
	  LOG.info("Inside Hive31ExploreService.getOperationInfo, QueryHandle:: " + queryHandle);
		return super.getOperationInfo(queryHandle);
	}
}
