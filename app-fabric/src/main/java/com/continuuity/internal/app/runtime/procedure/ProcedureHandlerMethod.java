package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.app.program.Program;
import com.continuuity.common.lang.InstantiatorFactory;
import com.continuuity.common.lang.PropertyFieldSetter;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.data2.transaction.TransactionExecutor;
import com.continuuity.data2.transaction.TransactionFailureException;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataSetFieldSetter;
import com.continuuity.internal.app.runtime.MetricsFieldSetter;
import com.continuuity.internal.lang.Reflections;
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
                         BasicProcedureContextFactory contextFactory) throws ClassNotFoundException {

    this.dataFabricFacade = dataFabricFacade;
    context = contextFactory.create(dataFabricFacade);

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
    context.getSystemMetrics().gauge("query.requests", 1);
    HandlerMethod handlerMethod = handlers.get(request.getMethod());
    if (handlerMethod == null) {
      LOG.error("Unsupport procedure method " + request.getMethod() + " on procedure " + procedure.getClass());
      context.getSystemMetrics().gauge("query.failures", 1);
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
      context.getSystemMetrics().gauge("query.processed", 1);
    } catch (Throwable t) {
      context.getSystemMetrics().gauge("query.failures", 1);
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
