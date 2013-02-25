package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.RunId;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.InstantiatorFactory;
import com.continuuity.internal.app.runtime.TransactionAgentSupplier;
import com.continuuity.internal.app.runtime.TransactionAgentSupplierFactory;
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
  private final Map<String, HandlerMethod> handlers;
  private final BasicProcedureContext context;

  ProcedureHandlerMethod(Program program, RunId runId, int instanceId,
                                TransactionAgentSupplierFactory txAgentSupplierFactory) throws ClassNotFoundException {

    TransactionAgentSupplier txAgentSupplier = txAgentSupplierFactory.create(program);
    DataSetContext dataSetContext = txAgentSupplier.getDataSetContext();

    ProcedureSpecification procedureSpec = program.getSpecification().getProcedures().get(program.getProgramName());
    context = new BasicProcedureContext(program, instanceId, runId,
                                        DataSets.createDataSets(dataSetContext, procedureSpec.getDataSets()),
                                        procedureSpec);

    TypeToken<? extends Procedure> procedureType = (TypeToken<? extends Procedure>)TypeToken.of(program.getMainClass());
    procedure = new InstantiatorFactory().get(procedureType).create();
    injectFields(procedure, procedureType, context);
    handlers = createHandlerMethods(procedure, procedureType, txAgentSupplier);
  }

  @Override
  public void handle(ProcedureRequest request, ProcedureResponder responder) {
    HandlerMethod handlerMethod = handlers.get(request.getMethod());
    if (handlerMethod == null) {
      LOG.error("Unsupport procedure method " + request.getMethod() + " on procedure " + procedure.getClass());
      context.getSystemMetrics().counter("query.failed", 1);
      try {
        responder.stream(new ProcedureResponse(ProcedureResponse.Code.NOT_FOUND));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      return;
    }

    try {
      handlerMethod.handle(request, responder);
      context.getSystemMetrics().counter("query.success", 1);
    } catch (Throwable t) {
      context.getSystemMetrics().counter("query.failed", 1);
      throw Throwables.propagate(t);
    }
  }

  private void injectFields(Procedure procedure, TypeToken<? extends Procedure> procedureType,
                            BasicProcedureContext context) {

    // Walk up the hierarchy of procedure class.
    for (TypeToken<?> type : procedureType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Inject DataSet and Metrics fields.
      for (Field field : type.getRawType().getDeclaredFields()) {
        // Inject DataSet
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset != null && !dataset.value().isEmpty()) {
            setField(procedure, field, context.getDataSet(dataset.value()));
          }
          continue;
        }
        if (Metrics.class.equals(field.getType())) {
          setField(procedure, field, context.getMetrics());
        }
      }
    }
  }

  private Map<String, HandlerMethod> createHandlerMethods(Procedure procedure,
                                                          TypeToken<? extends Procedure> procedureType,
                                                          TransactionAgentSupplier txAgentSupplier) {

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
          result.put(methodName, new ReflectionHandlerMethod(procedure, method, txAgentSupplier));
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
