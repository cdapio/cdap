/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.Process;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Controller;
import com.continuuity.app.runtime.Runner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class FlowletRunner implements Runner {

  @Override
  public Controller run(Program program, String name, Map<String, String> arguments) {
    try {
      Preconditions.checkArgument(program.getProcessorType() == Type.FLOW, "Supported process type");

      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      Type processorType = program.getProcessorType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.FLOW, "Only FLOW process type is supported.");

      String processorName = program.getProgramName();
      Preconditions.checkNotNull(processorName, "Missing processor name.");

      FlowSpecification flowSpec = appSpec.getFlows().get(processorName);
      FlowletDefinition flowletDef = flowSpec.getFlowlets().get(name);

      Preconditions.checkNotNull(flowletDef, "Definition missing for flowlet \"%s\"", name);
      Class<?> flowletClass = Class.forName(
                                             flowletDef.getFlowletSpec().getClassName(),
                                             true,
                                             program.getMainClass().getClassLoader()
      );

      Preconditions.checkArgument(Flowlet.class.isAssignableFrom(flowletClass), "%s is not a Flowlet.", flowletClass);


      //      instantiateFlowlet((Class<? extends Flowlet>) flowletClass);

    } catch(Exception e) {
      throw Throwables.propagate(e);
    }

    return null;
  }

  private Flowlet instantiateFlowlet(FlowSpecification flowSpec, FlowletDefinition flowletDef,
                                     String flowletName,
                                     Class<? extends Flowlet> flowletClass,
                                     DataSetContext dataSetCtx) throws Exception {

    TypeToken<? extends Flowlet> flowletType = TypeToken.of(flowletClass);
    Flowlet flowlet = flowletClass.newInstance();
    List<TransactionOutputEmitter<?>> outputEmitters = Lists.newArrayList();

    // Walk up the hierarchy of flowlet class.
    for (TypeToken<?> type : flowletType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Inject DataSet and Emitter fields
      for (Field field : type.getRawType().getDeclaredFields()) {
        // Inject DataSet
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset == null || dataset.value().isEmpty()) {
            continue;
          }
          if (!field.isAccessible()) {
            field.setAccessible(true);
          }
          field.set(flowlet, dataSetCtx.getDataSet(dataset.value()));
          continue;
        }
        // Inject OutputEmitter
        if (OutputEmitter.class.equals(field.getType())) {
          TypeToken<?> outputType = TypeToken.of(((ParameterizedType)field.getGenericType())
                                                   .getActualTypeArguments()[0]);
          String outputName = field.isAnnotationPresent(Output.class) ?
                    field.getAnnotation(Output.class).value() : FlowletDefinition.DEFAULT_OUTPUT;

          // TODO: Lookup queue name by output name
          // TODO: Find way to create QueueProducer
          URI queueName = URI.create(String.format("queue://%s/%s/%s", flowSpec.getName(), flowletDef.getFlowletSpec().getName(), outputName));

//          TransactionOutputEmitter<?> outputEmitter =
//            new ReflectionOutputEmitter(new QueueProducer(), queueName, flowletDef.getOutputs().get(outputName).iterator().next());
//          if (!field.isAccessible()) {
//            field.setAccessible(true);
//          }
//          field.set(flowlet, outputEmitter);
//          outputEmitters.add(outputEmitter);
        }
      }

      // Extracts all process methods
      for (Method method : type.getRawType().getDeclaredMethods()) {
        Process processAnnotation = method.getAnnotation(Process.class);
        if (!method.getName().startsWith(FlowletDefinition.PROCESS_METHOD_PREFIX) && processAnnotation == null) {
          continue;
        }

        List<String> inputNames;
        if (processAnnotation == null || processAnnotation.value().length == 0) {
          inputNames = ImmutableList.of(FlowletDefinition.ANY_INPUT);
        } else {
          inputNames = ImmutableList.copyOf(processAnnotation.value());
        }

        // TODO: Find out the queue name by input name
        // TODO: Create method invoker

      }
    }
    return flowlet;
  }
}
