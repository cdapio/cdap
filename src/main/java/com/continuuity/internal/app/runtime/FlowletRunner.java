/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Cancellable;
import com.continuuity.app.runtime.Runner;
import com.continuuity.filesystem.Location;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.util.Map;

/**
 *
 */
public final class FlowletRunner implements Runner {

  @Override
  public Cancellable run(Program program, String name, Map<String, String> arguments) {
    try {
      Preconditions.checkArgument(program.getProcessorType() == Type.FLOW, "Supported process type");

      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      Type processorType = program.getProcessorType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.FLOW, "Only FLOW process type is supported.");

      String processorName = program.getProcessorName();
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


  private <T extends Flowlet> T instantiateFlowlet(Class<? extends Flowlet> flowletClass,
                                                   DataSetContext dataSetCtx) throws Exception {

    //    TypeToken<? extends Flowlet> flowletType = TypeToken.of(flowletClass);
    //    Flowlet flowlet = flowletClass.newInstance();
    //
    //
    //    // Walk up the hierarchy of flowlet class.
    //    for (TypeToken<?> type : flowletType.getTypes().classes()) {
    //      if (type.getRawType().equals(Object.class)) {
    //        break;
    //      }
    //
    //      // Grab all the DataSet and OutputEmitter fields
    //      for (Field field : type.getRawType().getDeclaredFields()) {
    //        if (DataSet.class.isAssignableFrom(field.getType())) {
    //          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
    //          if (dataset == null || dataset.value().isEmpty()) {
    //            continue;
    //          }
    //
    //          // Inject DataSet object into flowlet
    //          field.set(flowlet, dataSetCtx.getDataSet(dataset.value()));
    //
    //        } else if (OutputEmitter.class.equals(field.getType())) {
    //          java.lang.reflect.Type emitterType = field.getGenericType();
    //          Preconditions.checkArgument(emitterType instanceof ParameterizedType,
    //                                      "Type info missing from OutputEmitter; class: %s; field: %s.", type, field);
    //
    //          // Extract the Output type from the first type argument of OutputEmitter
    //          java.lang.reflect.Type outputType = ((ParameterizedType) emitterType).getActualTypeArguments()[0];
    //          String outputName = field.isAnnotationPresent(Output.class) ?
    //            field.getAnnotation(Output.class).value() : DEFAULT_OUTPUT;
    //
    //          Set<java.lang.reflect.Type> types = outputs.get(outputName);
    //          if (types == null) {
    //            types = Sets.newHashSet();
    //            outputs.put(outputName, types);
    //          }
    //          types.add(outputType);
    //        }
    //      }
    //
    //      // Grab all process methods
    //      for (Method method : type.getRawType().getDeclaredMethods()) {
    //        com.continuuity.api.annotation.Process processAnnotation = method.getAnnotation(com.continuuity.api
    // .annotation.Process.class);
    //        if (!method.getName().startsWith(PROCESS_METHOD_PREFIX) && processAnnotation == null) {
    //          continue;
    //        }
    //
    //        java.lang.reflect.Type[] methodParams = method.getGenericParameterTypes();
    //        Preconditions.checkArgument(methodParams.length > 0 && methodParams.length <= 2,
    //                                    "Type parameter missing from process method; class: %s, method: %s",
    //                                    type, method);
    //
    //        // If there are more than one parameter, there be exactly two and the 2nd one should be InputContext
    //        if (methodParams.length == 2) {
    //          Preconditions.checkArgument(InputContext.class.equals(TypeToken.of(methodParams[1]).getRawType()),
    //                                      "The second parameter of the process method must be %s type.",
    //                                      InputContext.class.getName());
    //        }
    //
    //        // Extract the Input type from the first parameter of the process method
    //        java.lang.reflect.Type inputType = type.resolveType(methodParams[0]).getType();
    //
    //        List<String> inputNames = Lists.newLinkedList();
    //        if (processAnnotation == null || processAnnotation.value().length == 0) {
    //          inputNames.add(ANY_INPUT);
    //        } else {
    //          Collections.addAll(inputNames, processAnnotation.value());
    //        }
    //
    //        for (String inputName : inputNames) {
    //          Set<java.lang.reflect.Type> types = inputs.get(inputName);
    //          if (types == null) {
    //            types = Sets.newHashSet();
    //            inputs.put(inputName, types);
    //          }
    //          Preconditions.checkArgument(!types.contains(inputType),
    //                                      "Same type already defined for the same input. Type: %s, input: %s",
    //                                      inputType, inputName);
    //          types.add(inputType);
    //        }
    //      }
    //    }
    return null;
  }
}