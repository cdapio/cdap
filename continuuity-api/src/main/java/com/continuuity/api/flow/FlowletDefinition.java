/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow;

import com.continuuity.api.flow.flowlet.Flowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.internal.flowlet.DefaultFlowletSpecification;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.SchemaGenerator;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.internal.lang.Reflections;
import com.continuuity.internal.specification.DataSetFieldExtractor;
import com.continuuity.internal.specification.OutputEmitterFieldExtractor;
import com.continuuity.internal.specification.ProcessMethodExtractor;
import com.continuuity.internal.specification.PropertyFieldExtractor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
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
    Map<String, String> properties = Maps.newHashMap(flowletSpec.getProperties());
    Reflections.visit(flowlet, TypeToken.of(flowlet.getClass()),
                      new DataSetFieldExtractor(datasets),
                      new PropertyFieldExtractor(properties),
                      new OutputEmitterFieldExtractor(outputTypes),
                      new ProcessMethodExtractor(inputTypes));

    this.datasets = ImmutableSet.copyOf(datasets);
    this.inputTypes = immutableCopyOf(inputTypes);
    this.outputTypes = immutableCopyOf(outputTypes);
    this.flowletSpec = new DefaultFlowletSpecification(flowlet.getClass().getName(),
                                                       flowletName == null ? flowletSpec.getName() : flowletName,
                                                       flowletSpec.getDescription(), flowletSpec.getFailurePolicy(),
                                                       datasets, properties,
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

  /**
   * Creates a definition from a copy and overrides the stream connection.
   * @param definition definition to copy from
   * @param oldStreamInput stream connection to override
   * @param newStreamInput new value for input stream
   */
  public FlowletDefinition(FlowletDefinition definition, String oldStreamInput, String newStreamInput) {
    this(definition);

    // Changing what is going to be serialized and stored in MDS: intputs

    Set<Schema> schemas = inputs.get(oldStreamInput);
    if (schemas == null) {
      // This is fine: stream name was not in set in @ProcessInput, so no need to change it, as the change will
      // be reflected in Flow spec in flowlet connections
      return;
    }

    Schema streamSchema = null;
    Set<Schema> changedSchemas = Sets.newLinkedHashSet();
    for (Schema schema : schemas) {
      if (StreamEvent.class.getName().equals(schema.getRecordName())) {
        streamSchema = schema;
      } else {
        changedSchemas.add(schema);
      }
    }

    // Removing schema from set under old name (removing the whole set if nothing is left there)
    if (changedSchemas.isEmpty()) {
      inputs.remove(oldStreamInput);
    } else {
      inputs.put(oldStreamInput, changedSchemas);
    }

    // Adding schema to the set under new name (creating set if not exists)
    Set<Schema> newSchemas =
      inputs.containsKey(newStreamInput) ? inputs.get(newStreamInput) : Sets.<Schema>newLinkedHashSet();
    newSchemas.add(streamSchema);
    inputs.put(newStreamInput, newSchemas);
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

  private <K, V> Map<K, Set<V>> immutableCopyOf(Map<K, Set<V>> map) {
    Map<K, Set<V>> result = new HashMap<K, Set<V>>();
    for (Map.Entry<K, Set<V>> entry : map.entrySet()) {
      result.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
    }
    return result;
  }
}
