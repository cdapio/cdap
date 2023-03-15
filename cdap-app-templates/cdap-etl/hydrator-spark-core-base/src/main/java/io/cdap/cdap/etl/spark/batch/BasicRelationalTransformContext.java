/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Basic implementation of the {@link RelationalTranformContext} with single output port
 */
public class BasicRelationalTransformContext implements RelationalTranformContext {
  private final Engine engine;
  private final Map<String, Relation> inputMap;
  private final Map<String, Schema> inputSchemas;
  private final Schema outputSchema;
  private final FeatureFlagsProvider featureFlagsProvider;
  private Relation outputRelation;


  public BasicRelationalTransformContext(Engine engine,
                                         Map<String, Relation> inputMap,
                                         Map<String, Schema> inputSchemas,
                                         Schema outputSchema,
                                         FeatureFlagsProvider featureFlagsProvider) {
    this.engine = engine;
    this.inputMap = inputMap;
    this.inputSchemas = inputSchemas;
    this.outputSchema = outputSchema;
    this.featureFlagsProvider = featureFlagsProvider;
  }

  @Override
  public Engine getEngine() {
    return engine;
  }

  @Override
  public Relation getInputRelation(String inputStage) {
    return inputMap.get(inputStage);
  }

  @Override
  public Set<String> getInputRelationNames() {
    return Collections.unmodifiableSet(inputMap.keySet());
  }

  @Override
  public Schema getInputSchema(String inputStage) {
    return inputSchemas.get(inputStage);
  }

  @Override
  public Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  public void setOutputRelation(Relation outputRelation) {
    this.outputRelation = outputRelation;
  }

  @Override
  public void setOutputRelation(String portName, Relation outputDataSet) {
    throw new UnsupportedOperationException("Only single output is supported");
  }

  @Override
  public boolean isFeatureEnabled(String name) {
    return this.featureFlagsProvider.isFeatureEnabled(name);
  }

  public Relation getOutputRelation() {
    return outputRelation;
  }

  public Collection<Capability> getDefaultLanguageCapabilityList() {
    return Collections.singleton(StandardSQLCapabilities.POSTGRES);
  }
}
