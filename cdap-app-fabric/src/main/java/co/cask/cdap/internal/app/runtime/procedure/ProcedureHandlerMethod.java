/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.procedure.Procedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

/**
 *
 */
final class ProcedureHandlerMethod implements HandlerMethod {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureHandlerMethod.class);

  private static final String HANDLER_METHOD_PREFIX = "handle";
  private static final String ANY_METHOD = "";

  private final Procedure procedure;
  private final DataFabricFacade dataFabricFacade;
  private final Map<String, HandlerMethod> handlers;
  private final BasicProcedureContext context;

  ProcedureHandlerMethod(Program program, DataFabricFacade dataFabricFacade,
                         BasicProcedureContext context) throws ClassNotFoundException {

    this.dataFabricFacade = dataFabricFacade;
    this.context = context;

    try {
      TypeToken<? extends Procedure> procedureType = TypeToken.of(program.<Procedure>getMainClass());
      procedure = new InstantiatorFactory(false).get(procedureType).create();
      Reflections.visit(procedure, TypeToken.of(procedure.getClass()),
                        new PropertyFieldSetter(context.getSpecification().getProperties()),
                        new DataSetFieldSetter(context),
                        new MetricsFieldSetter(context.getMetrics()));

      handlers = createHandlerMethods(procedure, procedureType, dataFabricFacade);

      // TODO: It's a bit hacky, since we know there is one instance per execution handler thread.
      LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

    } catch (Throwable t) {
      // make sure the context releases all resources, datasets, ...
      context.close();
      throw Throwables.propagate(t);
    }
  }

  public Procedure getProcedure() {
    return procedure;
  }

  public BasicProcedureContext getContext() {
    return context;
  }

  public void init() {
    try {
      dataFabricFacade.createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          LOG.info("Initializing procedure: " + context);
          procedure.initialize(context);
          LOG.info("Procedure initialized: " + context);
        }
      });
    } catch (TransactionFailureException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      LOG.error("Procedure throws exception during init.", cause);
      // make sure the context releases all resources, datasets, ...
      context.close();
      throw Throwables.propagate(cause);
    } catch (InterruptedException e) {
      context.close();
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void handle(ProcedureRequest request, ProcedureResponder responder) {
    context.getProgramMetrics().increment("query.requests", 1);
    HandlerMethod handlerMethod = handlers.get(request.getMethod());
    if (handlerMethod == null) {
      LOG.error("Unsupport procedure method " + request.getMethod() + " on procedure " + procedure.getClass());
      context.getProgramMetrics().increment("query.failures", 1);
      try {
        responder.stream(new ProcedureResponse(ProcedureResponse.Code.NOT_FOUND));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      return;
    }

    try {
      ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(context.getProgram().getClassLoader());
      handlerMethod.handle(request, responder);
      Thread.currentThread().setContextClassLoader(oldClassLoader);
      context.getProgramMetrics().increment("query.processed", 1);
    } catch (Throwable t) {
      context.getProgramMetrics().increment("query.failures", 1);
      throw Throwables.propagate(t);
    }
  }

  private Map<String, HandlerMethod> createHandlerMethods(Procedure procedure,
                                                          TypeToken<? extends Procedure> procedureType,
                                                          DataFabricFacade dataFabricFacade) {

    ImmutableMap.Builder<String, HandlerMethod> result = ImmutableMap.builder();

    // Walk up the hierarchy of procedure class.
    for (TypeToken<?> type : procedureType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Gather all handler method
      for (Method method : type.getRawType().getDeclaredMethods()) {
        Handle handleAnnotation = method.getAnnotation(Handle.class);
        if (!method.getName().startsWith(HANDLER_METHOD_PREFIX) && handleAnnotation == null) {
          continue;
        }

        Set<String> methodNames;
        if (handleAnnotation == null || handleAnnotation.value().length == 0) {
          methodNames = ImmutableSet.of(ANY_METHOD);
        } else {
          methodNames = ImmutableSet.copyOf(handleAnnotation.value());
        }

        for (String methodName : methodNames) {
          result.put(methodName, new ReflectionHandlerMethod(procedure, method, dataFabricFacade));
        }
      }
    }

    return result.build();
  }

  private void setField(Procedure procedure, Field field, Object value) {
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    try {
      field.set(procedure, value);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }
}
