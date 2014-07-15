/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.services.legacy;

import com.continuuity.api.flow.flowlet.Flowlet;

/**
 * FlowletDefinitionModifier interfaces specifies what part of FlowletDefinition can be modified.
 * It is used internally for modifying flowlet specification.
 */
public interface FlowletDefinitionModifier {
  /**
   * Specifies new instance count for a flowlet.
   *
   * @param newInstances number of new instances of a flowlet
   * @return old instance count.
   */
  public int setInstances(int newInstances);

  /**
   * Set a new flowlet class that provides an implementation for this flowlet.
   *
   * @param clazz flowlet class to be associated with this flowlet.
   */
  public void setClazz(Class<? extends Flowlet> clazz);

  /**
   * Sets the type of flowlet in the flowlet definition.
   *
   * @param type of the flowlet.
   */
  public void setFlowletType(FlowletType type);

  /**
   * Sets the group id of the flowlet.
   *
   * @param groupId associated with flowlet.
   */
  public void setGroupId(long groupId);

}
