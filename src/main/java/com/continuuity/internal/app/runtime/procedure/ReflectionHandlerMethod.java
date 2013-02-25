package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.internal.app.runtime.TransactionAgentSupplier;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 *
 */
final class ReflectionHandlerMethod implements HandlerMethod {

  private static final Logger LOG = LoggerFactory.getLogger(ReflectionHandlerMethod.class);

  private final Procedure procedure;
  private final Method method;
  private final TransactionAgentSupplier txAgentSupplier;

  ReflectionHandlerMethod(Procedure procedure, Method method, TransactionAgentSupplier txAgentSupplier) {
    this.procedure = procedure;
    this.method = method;
    this.txAgentSupplier = txAgentSupplier;
  }

  @Override
  public void handle(ProcedureRequest request, ProcedureResponder responder) {
    TransactionAgent txAgent = txAgentSupplier.createAndUpdateProxy();

    try {
      txAgent.start();

      TransactionResponder txResponder = new TransactionResponder(txAgent, responder);
      try {
        method.invoke(procedure, request, txResponder);
      } catch (Throwable t) {
        LOG.error("Exception in calling procedure handler: " + method, t);
        try {
          txResponder.error(ProcedureResponse.Code.FAILURE, "Exception in procedure: " + t.getMessage());
        } catch (IOException e) {
          LOG.error("Fail to close response on error.", t);
        }
        throw Throwables.propagate(t);
      } finally {
        txResponder.close();
      }

    } catch (Exception e) {
      LOG.error("Handle method failure.", e);
      throw Throwables.propagate(e);
    }
  }
}
