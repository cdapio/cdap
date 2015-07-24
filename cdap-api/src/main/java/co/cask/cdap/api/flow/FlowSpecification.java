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

import co.cask.cdap.api.ProgramSpecification;

import java.util.List;
import java.util.Map;

/**
 * This class provides the specification of a Flow.
 *
 * @see co.cask.cdap.api.flow.flowlet.Flowlet Flowlet
 *
 */
public interface FlowSpecification extends ProgramSpecification {

  /**
   * @return Immutable Map from flowlet name to {@link FlowletDefinition}.
   */
  Map<String, FlowletDefinition> getFlowlets();

  /**
   * @return Immutable list of {@link FlowletConnection}s.
   */
  List<FlowletConnection> getConnections();
}
