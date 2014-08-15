/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.data.DataSetContext;
import co.cask.cdap.api.procedure.ProcedureContext;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data.dataset.DataSetInstantiator;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataSets;
import co.cask.cdap.internal.app.runtime.ProgramServiceDiscovery;
import org.apache.twill.api.RunId;

import java.io.Closeable;
import java.util.Map;

/**
 * Private interface to help creating {@link ProcedureContext}.
 */
final class BasicProcedureContextFactory {

  private final Program program;
  private final RunId runId;
  private final int instanceId;
  private final int instanceCount;
  private final Arguments userArguments;
  private final ProcedureSpecification procedureSpec;
  private final MetricsCollectionService collectionService;
  private final ProgramServiceDiscovery serviceDiscovery;

  BasicProcedureContextFactory(Program program, RunId runId, int instanceId, int instanceCount,
                               Arguments userArguments, ProcedureSpecification procedureSpec,
                               MetricsCollectionService collectionService, ProgramServiceDiscovery serviceDiscovery) {
    this.program = program;
    this.runId = runId;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.userArguments = userArguments;
    this.procedureSpec = procedureSpec;
    this.collectionService = collectionService;
    this.serviceDiscovery = serviceDiscovery;
  }

  BasicProcedureContext create(DataFabricFacade dataFabricFacade) {
    DataSetContext dataSetContext = dataFabricFacade.getDataSetContext();
    Map<String, Closeable> dataSets = DataSets.createDataSets(dataSetContext,
                                                            procedureSpec.getDataSets());
    BasicProcedureContext context = new BasicProcedureContext(program, runId, instanceId, instanceCount,
                                                              dataSets, userArguments, procedureSpec,
                                                              collectionService, serviceDiscovery);

    // hack for propagating metrics collector to datasets
    if (dataSetContext instanceof DataSetInstantiator) {
      ((DataSetInstantiator) dataSetContext).setMetricsCollector(collectionService, context.getSystemMetrics());
    }
    return context;
  }
}
