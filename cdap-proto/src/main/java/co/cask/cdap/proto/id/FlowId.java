/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.proto.id;

import co.cask.cdap.proto.ProgramType;

/**
 * Uniquely identifies a flow.
 */
public class FlowId extends ProgramId implements ParentedId<ApplicationId> {
  public FlowId(String namespace, String application, String flow) {
    super(namespace, application, ProgramType.FLOW, flow);
  }

  public FlowId(ApplicationId appId, String flow) {
    super(appId, ProgramType.FLOW, flow);
  }

  public static FlowId fromString(String string) {
    return EntityId.fromString(string, FlowId.class);
  }
}
