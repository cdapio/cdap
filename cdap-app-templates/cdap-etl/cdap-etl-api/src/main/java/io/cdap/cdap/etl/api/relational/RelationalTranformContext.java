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

import io.cdap.cdap.etl.api.TransformContext;

import java.util.Set;

/**
 * This interface provides sql engine, input relation(s) and a way to set tranformation results to
 * a {@link RelationalTransform#transform(RelationalTranformContext)} call
 */
public interface RelationalTranformContext {
  /**
   *
   * @return relational engine to be used for tranformation
   */
  Engine getEngine();

  /**
   *
   * @param inputStage input name
   * @return relation corresponding to the given input
   */
  Relation getInputRelation(String inputStage);

  /**
   *
   * @return set of all input relation names
   */
  Set<String> getInputRelationNames();

  /**
   * sets the primary output relation for the transform
   * @param outputRelation
   */
  void setOutputRelation(Relation outputRelation);

  /**
   *
   * @param portName
   * @param outputDataSet
   */
  void setOutputRelation(String portName, Relation outputDataSet);
}
