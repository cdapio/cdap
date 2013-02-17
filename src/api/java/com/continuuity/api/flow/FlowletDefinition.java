/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.Process;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.GeneratorFlowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.SchemaGenerator;
import com.continuuity.api.io.UnsupportedTypeException;
import com.continuuity.internal.api.flowlet.DefaultFlowletSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class defining the definition for a flowlet.
 */
public final class FlowletDefinition {
  private static final String PROCESS_METHOD_PREFIX = "process";
  private static final String DEFAULT_OUTPUT = "out";
  public static final String ANY_INPUT = "";

  private final FlowletSpecification flowletSpec;
  private final int instances;
  private final ResourceSpecification resourceSpec;
  private final Set<String> datasets;

  private final transient Map<String, Set<Type>> inputTypes;
  private final transient Map<String, Set<Type>> outputTypes;
  private Map<String, Set<Schema>> inputs;
  private Map<String, Set<Schema>> outputs;

  FlowletDefinition(Flowlet flowlet, int instances, ResourceSpecification resourceSpec) {
    this.flowletSpec = new DefaultFlowletSpecification(flowlet.getClass().getName(), flowlet.configure());
    this.instances = instances;
    this.resourceSpec = resourceSpec;

    Set<String> datasets = Sets.newHashSet();
    Map<String, Set<Type>> inputTypes = Maps.newHashMap();
    Map<String, Set<Type>> outputTypes = Maps.newHashMap();
    try {
      inspectFlowlet(flowlet.getClass(),datasets, inputTypes, outputTypes);
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException(e);
    }

    this.datasets = ImmutableSet.copyOf(datasets);
    this.inputTypes = immutableCopyOf(inputTypes);
    this.outputTypes = immutableCopyOf(outputTypes);
  }

  /**
   * @return Specification of Flowlet
   */
  public FlowletSpecification getFlowletSpec() {
    return flowletSpec;
  }

  /**
   * @return Number of instances configured for this flowlet.
   */
  public int getInstances() {
    return instances;
  }

  /**
   * @return Specification for resource.
   */
  public ResourceSpecification getResourceSpec() {
    return resourceSpec;
  }

  /**
   * @return Set of datasets names needed by this flowlet.
   */
  public Set<String> getDatasets() {
    return datasets;
  }

  /**
   * @return Mapping of name to the method types for processing inputs.
   */
  public Map<String, Set<Schema>> getInputs() {
    Preconditions.checkState(inputs != null, "Input schemas not yet generated.");
    return inputs;
  }

  /**
   * @return Mapping from name of {@link com.continuuity.api.flow.flowlet.OutputEmitter} to actual emitters.
   */
  public Map<String, Set<Schema>> getOutputs() {
    Preconditions.checkState(outputs != null, "Output schemas not yet generated.");
    return outputs;
  }

  /**
   * Generate schemas for all input and output types with the given {@link SchemaGenerator}.
   * @param generator The {@link SchemaGenerator} for generating type schema.
   */
  public void generateSchema(SchemaGenerator generator) throws UnsupportedTypeException {
    if (inputs == null && outputs == null && inputTypes != null && outputTypes != null) {
      // Generate both inputs and outputs before making this visible
      Map<String, Set<Schema>> inputs = generateSchema(generator, inputTypes, ImmutableMap.<String, Set<Schema>>builder());
      Map<String, Set<Schema>> outputs = generateSchema(generator, outputTypes, ImmutableMap.<String, Set<Schema>>builder());

      this.inputs = inputs;
      this.outputs = outputs;
    }
  }

  private Map<String, Set<Schema>> generateSchema(SchemaGenerator generator,
                                                  Map<String, Set<Type>> types,
                                                  ImmutableMap.Builder<String, Set<Schema>> result)
                                                  throws UnsupportedTypeException {
    for (Map.Entry<String, Set<Type>> entry : types.entrySet()) {
      ImmutableSet.Builder<Schema> schemas = ImmutableSet.builder();
      for (Type type : entry.getValue()) {
        schemas.add(generator.generate(type));
      }
      result.put(entry.getKey(), schemas.build());
    }
    return result.build();
  }

  /**
   * This method is responsible for inspecting the flowlet class and inspecting to figure out what
   * method are used for processing input and what are used for emitting outputs.
   * @param flowletClass defining the flowlet that needs to be inspected.
   * @param datasets reference to set of datasets names.
   * @param inputs reference to map of name to input types used for processing events on queues.
   * @param outputs reference to map of name to {@link OutputEmitter} and the types they handle.
   */
  private void inspectFlowlet(Class<?> flowletClass,
                              Set<String> datasets,
                              Map<String, Set<Type>> inputs,
                              Map<String, Set<Type>> outputs) throws UnsupportedTypeException {
    TypeToken<?> flowletType = TypeToken.of(flowletClass);

    // Walk up the hierarchy of flowlet class.
    for (TypeToken<?> type : flowletType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Grab all the DataSet and OutputEmitter fields
      for (Field field : type.getRawType().getDeclaredFields()) {
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset == null || dataset.value().isEmpty()) {
            continue;
          }
          datasets.add(dataset.value());

        } else if (OutputEmitter.class.equals(field.getType())) {
          Type emitterType = field.getGenericType();
          Preconditions.checkArgument(emitterType instanceof ParameterizedType,
                                      "Type info missing from OutputEmitter; class: %s; field: %s.", type, field);

          // Extract the Output type from the first type argument of OutputEmitter
          Type outputType = ((ParameterizedType) emitterType).getActualTypeArguments()[0];
          String outputName = field.isAnnotationPresent(Output.class) ?
                                  field.getAnnotation(Output.class).value() : DEFAULT_OUTPUT;

          Set<Type> types = outputs.get(outputName);
          if (types == null) {
            types = Sets.newHashSet();
            outputs.put(outputName, types);
          }
          types.add(outputType);
        }
      }

      // Grab all process methods
      for (Method method : type.getRawType().getDeclaredMethods()) {
        // There should be no process method on GeneratorFlowlet
        if (GeneratorFlowlet.class.isAssignableFrom(type.getRawType())) {
          continue;
        }
        Process processAnnotation = method.getAnnotation(Process.class);
        if (!method.getName().startsWith(PROCESS_METHOD_PREFIX) && processAnnotation == null) {
          continue;
        }

        Type[] methodParams = method.getGenericParameterTypes();
        Preconditions.checkArgument(methodParams.length > 0 && methodParams.length <= 2,
                                    "Type parameter missing from process method; class: %s, method: %s",
                                    type, method);

        // If there are more than one parameter, there be exactly two and the 2nd one should be InputContext
        if (methodParams.length == 2) {
          Preconditions.checkArgument(InputContext.class.equals(TypeToken.of(methodParams[1]).getRawType()),
                                      "The second parameter of the process method must be %s type.",
                                      InputContext.class.getName());
        }

        // Extract the Input type from the first parameter of the process method
        Type inputType = type.resolveType(methodParams[0]).getType();

        List<String> inputNames = Lists.newLinkedList();
        if (processAnnotation == null || processAnnotation.value().length == 0) {
          inputNames.add(ANY_INPUT);
        } else {
          Collections.addAll(inputNames, processAnnotation.value());
        }

        for (String inputName : inputNames) {
          Set<Type> types = inputs.get(inputName);
          if (types == null) {
            types = Sets.newHashSet();
            inputs.put(inputName, types);
          }
          Preconditions.checkArgument(!types.contains(inputType),
                                      "Same type already defined for the same input. Type: %s, input: %s",
                                      inputType, inputName);
          types.add(inputType);
        }
      }
    }
  }

  private <K,V> Map<K, Set<V>> immutableCopyOf(Map<K, Set<V>> map) {
    ImmutableMap.Builder<K, Set<V>> builder = ImmutableMap.builder();
    for (Map.Entry<K, Set<V>> entry : map.entrySet()) {
      builder.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
    }
    return builder.build();
  }
}
