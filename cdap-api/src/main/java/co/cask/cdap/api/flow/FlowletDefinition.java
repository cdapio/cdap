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

package co.cask.cdap.api.flow;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.flow.flowlet.FlowletSpecification;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.internal.flowlet.DefaultFlowletSpecification;
import co.cask.cdap.internal.io.SchemaGenerator;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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

  public FlowletDefinition(String flowletName, Map<String, Set<Type>> inputTypes, Map<String, Set<Type>> outputTypes,
                           FlowletSpecification flowletSpec, int instances) {
    this.instances = instances;
    this.datasets = Collections.unmodifiableSet(new HashSet<>(flowletSpec.getDataSets()));
    this.inputTypes = immutableCopyOf(inputTypes);
    this.outputTypes = immutableCopyOf(outputTypes);
    this.flowletSpec = new DefaultFlowletSpecification(flowletSpec.getClassName(),
                                                       flowletName == null ? flowletSpec.getName() : flowletName,
                                                       flowletSpec.getDescription(), flowletSpec.getFailurePolicy(),
                                                       flowletSpec.getDataSets(), flowletSpec.getProperties(),
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
    Set<Schema> changedSchemas = new LinkedHashSet<>();
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
    Set<Schema> newSchemas = inputs.containsKey(newStreamInput) ? inputs.get(newStreamInput) : new LinkedHashSet<>();
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
    if (inputs == null) {
      throw new IllegalStateException("Input schemas not yet generated.");
    }
    return inputs;
  }

  /**
   * @return Mapping from name of {@link co.cask.cdap.api.flow.flowlet.OutputEmitter} to actual emitters.
   */
  public Map<String, Set<Schema>> getOutputs() {
    if (outputs == null) {
      throw new IllegalStateException("Output schemas not yet generated.");
    }
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
    Map<String, Set<Schema>> result = new HashMap<>();
    for (Map.Entry<String, Set<Type>> entry : types.entrySet()) {
      Set<Schema> schemas = new LinkedHashSet<>();
      for (Type type : entry.getValue()) {
        schemas.add(generator.generate(type));
      }
      result.put(entry.getKey(), Collections.unmodifiableSet(schemas));
    }
    return result;
  }

  private <K, V> Map<K, Set<V>> immutableCopyOf(Map<K, Set<V>> map) {
    Map<K, Set<V>> result = new HashMap<>();
    for (Map.Entry<K, Set<V>> entry : map.entrySet()) {
      result.put(entry.getKey(), Collections.unmodifiableSet(new HashSet<>(entry.getValue())));
    }
    return result;
  }
}
