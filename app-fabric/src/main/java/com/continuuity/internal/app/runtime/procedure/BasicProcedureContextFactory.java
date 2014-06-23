package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.dataset.DataSetInstantiationBase;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
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
    if (dataSetContext instanceof DataSetInstantiationBase) {
      ((DataSetInstantiationBase) dataSetContext).setMetricsCollector(collectionService, context.getSystemMetrics());
    }
    return context;
  }
}
