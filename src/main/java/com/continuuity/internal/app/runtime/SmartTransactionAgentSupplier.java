package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.DataSetContext;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SmartTransactionAgent;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 * A {@link TransactionAgentSupplier} that will create a new {@link SmartTransactionAgent} every time
 * when the {@link #createAndUpdateProxy} method is called. Also the newly created {@link TransactionAgent} would be set
 * into the given {@link TransactionProxy} instance.
 */
public final class SmartTransactionAgentSupplier implements TransactionAgentSupplier {

  private final OperationExecutor opex;
  private final Program program;
  private final TransactionProxy transactionProxy;
  private final DataSetContext dataSetContext;

  @Inject
  public SmartTransactionAgentSupplier(OperationExecutor opex,
                                       @Assisted Program program) {
    this.opex = opex;
    this.program = program;
    this.transactionProxy = new TransactionProxy();
    this.dataSetContext = createDataSetContext(program, opex, transactionProxy);
  }

  @Override
  public DataSetContext getDataSetContext() {
    return dataSetContext;
  }

  @Override
  public TransactionAgent createAndUpdateProxy() {
    OperationContext ctx = new OperationContext(program.getAccountId(), program.getApplicationId());
    TransactionAgent agent = new SmartTransactionAgent(opex, ctx);
    transactionProxy.setTransactionAgent(agent);
    return agent;
  }

  @Override
  public TransactionAgent create() {
    OperationContext ctx = new OperationContext(program.getAccountId(), program.getApplicationId());
    return new SmartTransactionAgent(opex, ctx);
  }

  private DataSetContext createDataSetContext(Program program, OperationExecutor opex, TransactionProxy proxy) {
    try {
      OperationContext ctx = new OperationContext(program.getAccountId(), program.getApplicationId());
      DataFabric dataFabric = new DataFabricImpl(opex, ctx);
      DataSetInstantiator dataSetInstantiator = new DataSetInstantiator(dataFabric, proxy,
                                                                        program.getMainClass().getClassLoader());
      if (program.getProcessorType() == Type.PROCEDURE) {
        dataSetInstantiator.setReadOnly();
      }
      dataSetInstantiator.setDataSets(ImmutableList.copyOf(program.getSpecification().getDataSets().values()));
      return dataSetInstantiator;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
