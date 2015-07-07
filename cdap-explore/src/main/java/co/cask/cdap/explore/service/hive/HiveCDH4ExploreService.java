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

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.dataset.SystemDatasetInstantiatorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryStatus;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Hive patched for CDH4 implementation of {@link co.cask.cdap.explore.service.ExploreService}.
 * There are 3 changes compared to Hive 13 implementation -
 * <ol>
 *   <li>{@link org.apache.hive.service.cli.CLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)}
 *   return type has changed</li>
 *   <li>{@link org.apache.hive.service.cli.CLIService#fetchResults(org.apache.hive.service.cli.OperationHandle)}
 *   return type has changed</li>
 *   <li>{@link org.apache.hive.service.cli.CLIService#executeStatementAsync(org.apache.hive.service.cli.SessionHandle,
 *   String, java.util.Map)} does not exist, only {@link org.apache.hive.service.cli.CLIService
 *   #executeStatement(org.apache.hive.service.cli.SessionHandle, String, java.util.Map)}</li>
 * </ol>
 */
public class HiveCDH4ExploreService extends Hive13ExploreService {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCDH4ExploreService.class);

  @Inject
  protected HiveCDH4ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                                   CConfiguration cConf, Configuration hConf, HiveConf hiveConf,
                                   @Named(Constants.Explore.PREVIEWS_DIR_NAME) File previewsDir,
                                   StreamAdmin streamAdmin, Store store,
                                   SystemDatasetInstantiatorFactory datasetInstantiatorFactory) {
    super(txClient, datasetFramework, cConf, hConf, hiveConf, previewsDir,
          streamAdmin, store, datasetInstantiatorFactory);
    System.setProperty("hive.server2.blocking.query", "false");
    if (cConf.getBoolean(Constants.Explore.WRITES_ENABLED)) {
      LOG.warn("Writing to datasets through Hive is not supported in CDH4.x, overriding {} setting to false.",
               Constants.Explore.WRITES_ENABLED);
      cConf.setBoolean(Constants.Explore.WRITES_ENABLED, false);
    }
  }

  @Override
  protected QueryStatus fetchStatus(OperationHandle operationHandle)
    throws HiveSQLException, ExploreException, HandleNotFoundException {
    try {
      // In Hive patched for CDH4, CLIService.getOperationStatus returns OperationState.
      // In Hive 13, CLIService.getOperationStatus returns OperationStatus.
      // Since we currently compile using Hive 13, we need the following workaround to get
      // Hive patched for CDH4 working, so that the compiler does not make assumption
      // on the return type of fetchResults

      Class cliServiceClass = getCliService().getClass();
      Method m = cliServiceClass.getMethod("getOperationStatus", OperationHandle.class);
      OperationState operationState = (OperationState) m.invoke(getCliService(), operationHandle);
      return new QueryStatus(QueryStatus.OpStatus.valueOf(operationState.toString()), operationHandle.hasResultSet());
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e);
    } catch (NoSuchMethodException e) {
      throw  Throwables.propagate(e);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void setCurrentDatabase(String dbName) throws IOException, HiveSQLException, ExploreException {
    final OperationHandle opHandle = doExecute(getCliService().openSession("", "", startSession()), "USE " + dbName);
    try {
      Tasks.waitFor(QueryStatus.OpStatus.FINISHED, new Callable<QueryStatus.OpStatus>() {
        @Override
        public QueryStatus.OpStatus call() throws Exception {
          return fetchStatus(opHandle).getStatus();
        }
      }, 5, TimeUnit.SECONDS, 200, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      // Rethrow exception and don't execute query i.e. return 500
      throw Throwables.propagate(e);
    }
  }

  @Override
  void cancelInternal(QueryHandle handle) throws ExploreException, HandleNotFoundException, SQLException {
    LOG.warn("Trying to cancel operation with handle {}", handle);
    throw new UnsupportedOperationException("Cancel operation is not supported with CDH4.");
  }
}
