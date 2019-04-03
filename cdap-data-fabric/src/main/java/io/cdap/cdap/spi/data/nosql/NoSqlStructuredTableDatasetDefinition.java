/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.nosql;

import com.google.common.base.Throwables;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.data2.dataset2.DefaultDatasetRuntimeContext;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAuthorizer;

import java.io.IOException;
import java.util.Map;

/**
 * Wrapper class for use in NoSqlStructuredTable classes to ensure the correct context is set for the dataset class
 * loader.
 */
public class NoSqlStructuredTableDatasetDefinition implements DatasetDefinition {

  private static final AuthorizationEnforcer SYSTEM_NAMESPACE_ENFORCER = new NoOpAuthorizer();
  private static final DefaultDatasetRuntimeContext.DatasetAccessRecorder SYSTEM_NAMESPACE_ACCESS_RECORDER =
    new DefaultDatasetRuntimeContext.DatasetAccessRecorder() {
      @Override
      public void recordLineage(AccessType accessType) {
        // no-op
      }

      @Override
      public void emitAudit(AccessType accessType) {
        // no-op
      }
    };
  private final DatasetDefinition datasetDefinition;

  public NoSqlStructuredTableDatasetDefinition(DatasetDefinition datasetDefinition) {
    this.datasetDefinition = datasetDefinition;
  }

  @Override
  public String getName() {
    return datasetDefinition.getName();
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    return datasetDefinition.configure(instanceName, properties);
  }

  @Override
  public DatasetAdmin getAdmin(DatasetContext datasetContext, DatasetSpecification spec, ClassLoader classLoader)
    throws IOException {
    return datasetDefinition.getAdmin(datasetContext, spec, classLoader);
  }

  @Override
  public Dataset getDataset(DatasetContext datasetContext, DatasetSpecification spec, Map arguments,
                            ClassLoader classLoader) throws IOException {
    try {
      return DefaultDatasetRuntimeContext.execute(SYSTEM_NAMESPACE_ENFORCER, SYSTEM_NAMESPACE_ACCESS_RECORDER,
                                                  null, null, null,
                                                  () -> datasetDefinition.getDataset(
                                                    datasetContext, spec, arguments, classLoader));
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }
}
