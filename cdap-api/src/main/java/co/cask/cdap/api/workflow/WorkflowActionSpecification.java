/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.api.workflow;

import co.cask.cdap.api.common.PropertyProvider;
import co.cask.cdap.api.customaction.CustomActionSpecification;
import co.cask.cdap.api.dataset.Dataset;

import java.util.Set;

/**
 * Specification for a {@link WorkflowAction}.
 * @deprecated Deprecated as of 3.5.0. Please use {@link CustomActionSpecification} instead.
 */
@Deprecated
public interface WorkflowActionSpecification extends PropertyProvider {

  /**
   * @return Class name of the workflow action.
   */
  String getClassName();

  /**
   * @return Name of the workflow action.
   */
  String getName();

  /**
   * @return Description of the workflow action.
   */
  String getDescription();

  /**
   * @return an immutable set of {@link Dataset} name that are used by the {@link WorkflowAction}
   */
  Set<String> getDatasets();
}
