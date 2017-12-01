/*
 * Copyright © 2017 Cask Data, Inc.
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
 * Uniquely identifies a worker.
 */
public class WorkerId extends ProgramId implements ParentedId<ApplicationId> {
  public WorkerId(String namespace, String application, String program) {
    super(namespace, application, ProgramType.WORKER, program);
  }

  public WorkerId(ApplicationId appId, String program) {
    super(appId, ProgramType.WORKER, program);
  }

  public WorkerId(ProgramId programId) {
    super(programId.getParent(), ProgramType.WORKER, programId.getEntityName());
  }

  public static WorkerId fromString(String string) {
    return EntityId.fromString(string, WorkerId.class);
  }

}
