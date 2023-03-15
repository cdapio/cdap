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

package io.cdap.cdap.etl.api.relational;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import java.util.Collection;
import java.util.Set;

/**
 * This interface provides sql engine, input relation(s) and a way to set tranformation results to a
 * {@link RelationalTransform#transform(RelationalTranformContext)} call.
 */
public interface RelationalTranformContext extends FeatureFlagsProvider {

  /**
   * Gets the relational engine to be used for the transformation.
   *
   * @return relational engine to be used for transformation
   */
  Engine getEngine();

  /**
   * Gets the relation corresponding to the given input.
   *
   * @param inputStage input name
   * @return relation corresponding to the given input
   */
  Relation getInputRelation(String inputStage);

  /**
   * Gets the set of all relation names in the input.
   *
   * @return set of all input relation names
   */
  Set<String> getInputRelationNames();

  /**
   * Gets schema for input stage.
   *
   * @param inputStage input name
   * @return relation corresponding to the given input
   */
  Schema getInputSchema(String inputStage);

  /**
   * Gets the output schema for this transform context.
   *
   * @return output schema
   */
  Schema getOutputSchema();

  /**
   * Sets the primary output relation for the transform.
   *
   * @param outputRelation the output relation
   */
  void setOutputRelation(Relation outputRelation);

  /**
   * Sets the output relation corresponding to an output stage.
   *
   * @param portName name of output stage
   * @param outputDataSet output relation
   */
  void setOutputRelation(String portName, Relation outputDataSet);

  /**
   * Provides a list of capabilities that plugins must use to locate an ExpressionFactory to parse user-entered
   * expressions.
   *
   * @return the list of languages supported by default as a list of {@link Capability}s.
   */
  default Collection<Capability> getDefaultLanguageCapabilityList() {
    throw new UnsupportedOperationException("Default language capabilities are not supported for this relation "
            + "transform context.");
  }
}
