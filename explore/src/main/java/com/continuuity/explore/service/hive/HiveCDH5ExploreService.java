/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.explore.service.hive;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.proto.QueryStatus;
import com.continuuity.tephra.TransactionSystemClient;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.SessionHandle;

/**
 * Hive 13 patched for CDH5 implementation of {@link com.continuuity.explore.service.ExploreService}.
 * There is only 1 change compared to Hive 13 implementation -
 * <ol>
 *   <li>{@link org.apache.hive.service.cli.CLIService#getOperationStatus(org.apache.hive.service.cli.OperationHandle)}
 *   return type has changed</li>
 * </ol>
 */
public class HiveCDH5ExploreService extends BaseHiveExploreService {

  @Inject
  protected HiveCDH5ExploreService(TransactionSystemClient txClient, DatasetFramework datasetFramework,
                                   CConfiguration cConf, Configuration hConf, HiveConf hiveConf) {
    super(txClient, datasetFramework, cConf, hConf, hiveConf);
  }

  @Override
  protected QueryStatus fetchStatus(OperationHandle handle)
    throws HiveSQLException, ExploreException, HandleNotFoundException {
    OperationStatus operationStatus = getCliService().getOperationStatus(handle);
    return new QueryStatus(QueryStatus.OpStatus.valueOf(operationStatus.getState().toString()),
                      handle.hasResultSet());
  }

  @Override
  protected OperationHandle doExecute(SessionHandle sessionHandle, String statement)
    throws HiveSQLException, ExploreException {
    return getCliService().executeStatementAsync(sessionHandle, statement, ImmutableMap.<String, String>of());
  }
}
