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

package co.cask.cdap.api.flow.flowlet;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.PropertyProvider;

import java.util.Set;

/**
 * This class provides specification of a Flowlet.
 */
public interface FlowletSpecification extends PropertyProvider {

  /**
   * @return Class name of the {@link Flowlet} class.
   */
  String getClassName();

  /**
   * @return Name of the flowlet.
   */
  String getName();

  /**
   * @return Description of the flowlet.
   */
  String getDescription();

  /**
   * @return The failure policy of the flowlet.
   */
  FailurePolicy getFailurePolicy();

  /**
   * @return An immutable set of {@link co.cask.cdap.api.dataset.Dataset DataSets} name that
   *         used by the {@link Flowlet}.
   */
  Set<String> getDataSets();

  /**
   * @return The {@link Resources} requirements for the flowlet.
   */
  Resources getResources();
}
