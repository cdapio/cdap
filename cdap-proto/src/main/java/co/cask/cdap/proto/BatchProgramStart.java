/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.proto;

import java.util.Collections;
import java.util.Map;

/**
 * Array components of the batch start request to POST /namespaces/{namespace}/start.
 */
public class BatchProgramStart extends BatchProgram {
  private final Map<String, String> runtimeargs;

  public BatchProgramStart(BatchProgram program) {
    this(program, Collections.<String, String>emptyMap());
  }

  public BatchProgramStart(BatchProgram program, Map<String, String> runtimeargs) {
    this(program.getAppId(), program.getProgramType(), program.getProgramId(), runtimeargs);
  }

  public BatchProgramStart(String appId, ProgramType programType, String programId) {
    this(appId, programType, programId, Collections.<String, String>emptyMap());
  }

  public BatchProgramStart(String appId, ProgramType programType, String programId, Map<String, String> runtimeargs) {
    super(appId, programType, programId);
    this.runtimeargs = runtimeargs;
  }

  public Map<String, String> getRuntimeargs() {
    // null check here since this object is often created through gson deserialization
    return runtimeargs == null ? Collections.<String, String>emptyMap() : runtimeargs;
  }
}
