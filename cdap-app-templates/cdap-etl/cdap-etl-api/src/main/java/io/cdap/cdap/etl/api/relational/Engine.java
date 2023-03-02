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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Engine interface provides access to functions common to all relations used in the tranformation
 * request. E.g. it provides expression factories and engine capabilities.
 */
public interface Engine {

  /**
   * @return capabilities provided by the engine and it's expression factories
   */
  Set<Capability> getCapabilities();

  /**
   * @return all expression factories provided by the engine
   */
  List<ExpressionFactory<?>> getExpressionFactories();

  /**
   * Returns expression factory of specified type with required capabilities Example:
   * getExpressionFactory(StringExpressionFactory.SQL)
   *
   * @param type expression factory type
   * @param neededCapabilities capabilities that factory should implement
   * @return a factory or empty optional if engine does not provide such factory
   */
  default <T> Optional<ExpressionFactory<T>> getExpressionFactory(
      ExpressionFactoryType<T> type,
      Capability... neededCapabilities) {
    return getExpressionFactory(type, Arrays.asList(neededCapabilities));
  }

  /**
   * Returns expression factory of specified type with required capabilities Example:
   * getExpressionFactory(StringExpressionFactory.SQL)
   *
   * @param type expression factory type
   * @param neededCapabilities capabilities that factory should implement
   * @return a factory or empty optional if engine does not provide such factory
   */
  default <T> Optional<ExpressionFactory<T>> getExpressionFactory(
      ExpressionFactoryType<T> type,
      Collection<Capability> neededCapabilities) {

    return getExpressionFactories().stream()
        .filter(f -> f.getType() == type && f.getCapabilities().containsAll(neededCapabilities))
        .map(f -> (ExpressionFactory<T>) f)
        .findFirst();
  }
}
