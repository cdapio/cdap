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

import co.cask.cdap.api.procedure.Procedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import com.continuuity.tephra.TransactionContext;
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
  private final DataFabricFacade dataFabricFacade;

  ReflectionHandlerMethod(Procedure procedure, Method method, DataFabricFacade dataFabricFacade) {
    this.procedure = procedure;
    this.method = method;
    this.dataFabricFacade = dataFabricFacade;

    if (!this.method.isAccessible()) {
      this.method.setAccessible(true);
    }
  }

  @Override
  public void handle(ProcedureRequest request, ProcedureResponder responder) {
    TransactionContext txContext = dataFabricFacade.createTransactionManager();

    try {
      txContext.start();

      TransactionResponder txResponder = new TransactionResponder(txContext, responder);
      try {
        method.invoke(procedure, request, txResponder);
      } catch (Throwable t) {
        LOG.error("Exception in calling procedure handler: " + method, t);
        try {
          Throwable cause = t.getCause();
          txResponder.error(ProcedureResponse.Code.FAILURE,
                            cause + " at " + getFirstStackTrace(cause));
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

  private String getFirstStackTrace(Throwable cause) {
    if (cause == null) {
      return "";
    }
    StackTraceElement[] stackTrace = cause.getStackTrace();
    if (stackTrace.length <= 0) {
      return "";
    }
    StackTraceElement element = stackTrace[0];
    return String.format("%s.%s(%s:%d)",
                         element.getClassName(), element.getMethodName(),
                         element.getFileName(), element.getLineNumber());
  }
}
