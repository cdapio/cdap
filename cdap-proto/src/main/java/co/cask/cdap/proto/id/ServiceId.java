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

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;

/**
 * Uniquely identifies a service.
 */
public class ServiceId extends ProgramId implements ParentedId<ApplicationId> {
  public ServiceId(String namespace, String application, String program) {
    super(namespace, application, ProgramType.SERVICE, program);
  }

  public ServiceId(ApplicationId appId, String program) {
    super(appId, ProgramType.SERVICE, program);
  }

  public ServiceId(ProgramId programId) {
    super(programId.getParent(), ProgramType.SERVICE, programId.getEntityName());
  }

  @Override
  public Id.Service toId() {
    return Id.Service.from(super.getParent().toId(), super.getProgram());
  }

  public static ServiceId fromString(String string) {
    return EntityId.fromString(string, ServiceId.class);
  }
}
