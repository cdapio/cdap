/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

import com.continuuity.api.ResourceSpecification;
import com.continuuity.api.annotation.Batch;
import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.Tick;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.internal.flowlet.DefaultFlowletSpecification;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaGenerator;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class defining the definition for a flowlet.
 */
public final class FlowletDefinition {
  public static final String DEFAULT_OUTPUT = "queue";
  public static final String ANY_INPUT = "";

  private final FlowletSpecification flowletSpec;
  private int instances;
  private final Set<String> datasets;

  private final transient Map<String, Set<Type>> inputTypes;
  private final transient Map<String, Set<Type>> outputTypes;
  private Map<String, Set<Schema>> inputs;
  private Map<String, Set<Schema>> outputs;

  FlowletDefinition(String flowletName, Flowlet flowlet, int instances) {
    FlowletSpecification flowletSpec = flowlet.configure();

    this.instances = instances;

    Set<String> datasets = Sets.newHashSet(flowletSpec.getDataSets());
    Map<String, Set<Type>> inputTypes = Maps.newHashMap();
    Map<String, Set<Type>> outputTypes = Maps.newHashMap();
    try {
      inspectFlowlet(flowlet.getClass(), datasets, inputTypes, outputTypes);
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException(e);
    }

    this.datasets = ImmutableSet.copyOf(datasets);
    this.inputTypes = immutableCopyOf(inputTypes);
    this.outputTypes = immutableCopyOf(outputTypes);

    this.flowletSpec = new DefaultFlowletSpecification(flowlet.getClass().getName(),
                                                       flowletName == null ? flowletSpec.getName() : flowletName,
                                                       flowletSpec.getDescription(), flowletSpec.getFailurePolicy(),
                                                       datasets, flowletSpec.getArguments(),
                                                       flowletSpec.getResources());
  }

  /**
   * Creates a definition from a copy and overrides the number of instances.
   * @param definition definition to copy from
   * @param instances new number of instances
   */
  public FlowletDefinition(FlowletDefinition definition, int instances) {
    this(definition);
    this.instances = instances;
  }

  private FlowletDefinition(FlowletDefinition definition) {
    this.flowletSpec = definition.flowletSpec;
    this.instances = definition.instances;
    this.datasets = definition.datasets;
    this.inputTypes = definition.inputTypes;
    this.outputTypes = definition.outputTypes;
    this.inputs = definition.inputs;
    this.outputs = definition.outputs;
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
   * @return Set of dataset names needed by this flowlet.
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
      Map<String, Set<Schema>> inputs = generateSchema(generator, inputTypes);
      Map<String, Set<Schema>> outputs = generateSchema(generator, outputTypes);

      this.inputs = inputs;
      this.outputs = outputs;
    }
  }

  private Map<String, Set<Schema>> generateSchema(SchemaGenerator generator, Map<String, Set<Type>> types)
                                                  throws UnsupportedTypeException {
    Map<String, Set<Schema>> result = new HashMap<String, Set<Schema>>();
    for (Map.Entry<String, Set<Type>> entry : types.entrySet()) {
      ImmutableSet.Builder<Schema> schemas = ImmutableSet.builder();
      for (Type type : entry.getValue()) {
        schemas.add(generator.generate(type));
      }
      result.put(entry.getKey(), schemas.build());
    }
    return result;
  }

  /**
   * This method is responsible for inspecting the flowlet class and inspecting to figure out what
   * methods are used for processing input and what are used for emitting outputs.
   * @param flowletClass Defines the flowlet that needs to be inspected.
   * @param datasets The reference to a set of datasets names.
   * @param inputs The reference to a map of name to input types used for processing events on queues.
   * @param outputs The reference to a map of name to {@link OutputEmitter} and the types they handle.
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

      // Grab all of the DataSet and OutputEmitter fields.
      for (Field field : type.getRawType().getDeclaredFields()) {
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset == null || dataset.value().isEmpty()) {
            continue;
          }
          datasets.add(dataset.value());

        } else if (OutputEmitter.class.equals(field.getType())) {
          Type emitterType = flowletType.resolveType(field.getGenericType()).getType();
          checkArgument(emitterType instanceof ParameterizedType, type, field, "Type info missing from OutputEmitter.");

          // Extract the Output type from the first type argument of OutputEmitter.
          Type outputType = ((ParameterizedType) emitterType).getActualTypeArguments()[0];
          outputType = flowletType.resolveType(outputType).getType();
          String outputName = field.isAnnotationPresent(Output.class) ?
                                  field.getAnnotation(Output.class).value() : DEFAULT_OUTPUT;

          checkType(outputType, type, field);

          Set<Type> types = outputs.get(outputName);
          if (types == null) {
            types = Sets.newHashSet(outputType);
            outputs.put(outputName, types);
          } else {
            // Queue name is constructed by flowletname+outputname, hence only one type object can be emitted.
            throw new IllegalArgumentException(
              String.format("Same output name cannot have more than one type. Use @Output; class: %s, field: %s",
                            type, field));
          }
        }
      }

      // Grab all process methods
      for (Method method : type.getRawType().getDeclaredMethods()) {
        ProcessInput processInputAnnotation = method.getAnnotation(ProcessInput.class);
        Tick tickAnnotation = method.getAnnotation(Tick.class);

        if (processInputAnnotation == null && tickAnnotation == null) {
          continue;
        }

        // Check for tick method
        if (tickAnnotation != null) {
          checkArgument(processInputAnnotation == null, type, method, "Tick method should not have ProcessInput.");
          checkArgument(method.getParameterTypes().length == 0, type, method, "Tick method cannot have parameters.");
          continue;
        }

        // A process method cannot be a tick method
        checkArgument(tickAnnotation == null, type, method,
                      "ProcessInput method cannot be Tick method.");

        Type[] methodParams = method.getGenericParameterTypes();
        checkArgument(methodParams.length > 0 && methodParams.length <= 2, type, method,
                      "Type parameter missing from process method.");

        // If there is more than one parameter there can only be exactly two; the second one must be InputContext type
        if (methodParams.length == 2) {
          checkArgument(InputContext.class.equals(TypeToken.of(methodParams[1]).getRawType()), type, method,
                        "The second parameter of the process method must be InputContext type.");
        }

        Type firstParameter = type.resolveType(methodParams[0]).getType();

        // In batch mode, if the first parameter is an iterator then extract the type information from
        // the iterator's type parameter
        if (method.getAnnotation(Batch.class) != null) {
          checkArgument(firstParameter instanceof ParameterizedType, type, method,
                        "Iterator needs to be a ParameterizedType to extract type information.");

          ParameterizedType pType = (ParameterizedType) firstParameter;
          checkArgument(pType.getRawType().equals(Iterator.class), type, method,
                        "Batch mode without an Iterator as first parameter is not supported yet.");
          checkArgument(pType.getActualTypeArguments().length > 0, type, method,
                        "Iterator does not define actual type parameters, cannot extract type information.");
          firstParameter = pType.getActualTypeArguments()[0];
        }

        // Extract the Input type from the first parameter of the process method
        Type inputType = type.resolveType(firstParameter).getType();
        checkType(inputType, type, method);

        List<String> inputNames = Lists.newLinkedList();
        if (processInputAnnotation == null || processInputAnnotation.value().length == 0) {
          inputNames.add(ANY_INPUT);
        } else {
          Collections.addAll(inputNames, processInputAnnotation.value());
        }

        for (String inputName : inputNames) {
          Set<Type> types = inputs.get(inputName);
          if (types == null) {
            types = Sets.newHashSet();
            inputs.put(inputName, types);
          }
          checkArgument(types.add(inputType), type, method, "Same type already defined for the same input.");
        }
      }
    }
  }

  private <T> void checkArgument(boolean condition, TypeToken<?> type, T context, String errorMsg) {
    Preconditions.checkArgument(condition,  "%s. Class: %s, context: %s", errorMsg, type, context);
  }

  private <T> void checkType(Type type, TypeToken<?> clz, T context) {
    if (type instanceof GenericArrayType) {
      checkType(((GenericArrayType) type).getGenericComponentType(), clz, context);
      return;
    }
    checkArgument(type instanceof Class || type instanceof ParameterizedType, clz, context,
                  "Invalid type. Only Class or ParameterizedType are supported.");
  }

  private <K, V> Map<K, Set<V>> immutableCopyOf(Map<K, Set<V>> map) {
    Map<K, Set<V>> result = new HashMap<K, Set<V>>();
    for (Map.Entry<K, Set<V>> entry : map.entrySet()) {
      result.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
    }
    return result;
  }
}
