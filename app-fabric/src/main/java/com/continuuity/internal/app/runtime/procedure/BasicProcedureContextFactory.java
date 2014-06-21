package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.procedure.ProcedureContext;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.dataset.DataSetInstantiationBase;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataSets;
import org.apache.twill.api.RunId;
import org.apache.twill.zookeeper.ZKClientService;

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
  private final ZKClientService zkClientService;
  private final CConfiguration cConf;


  BasicProcedureContextFactory(Program program, RunId runId, int instanceId, int instanceCount,
                               Arguments userArguments, ProcedureSpecification procedureSpec,
                               MetricsCollectionService collectionService, ZKClientService zkClientService,
                               CConfiguration cConf) {
    this.program = program;
    this.runId = runId;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.userArguments = userArguments;
    this.procedureSpec = procedureSpec;
    this.collectionService = collectionService;
    this.zkClientService = zkClientService;
    this.cConf = cConf;
  }

  BasicProcedureContext create(DataFabricFacade dataFabricFacade) {
    DataSetContext dataSetContext = dataFabricFacade.getDataSetContext();
    Map<String, Closeable> dataSets = DataSets.createDataSets(dataSetContext,
                                                            procedureSpec.getDataSets());
    BasicProcedureContext context = new BasicProcedureContext(program, runId, instanceId, instanceCount,
                                                              dataSets, userArguments, procedureSpec,
                                                              collectionService, zkClientService, cConf);

    // hack for propagating metrics collector to datasets
    if (dataSetContext instanceof DataSetInstantiationBase) {
      ((DataSetInstantiationBase) dataSetContext).setMetricsCollector(collectionService, context.getSystemMetrics());
    }
    return context;
  }
}
