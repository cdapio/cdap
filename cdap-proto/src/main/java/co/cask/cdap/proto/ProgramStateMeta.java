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

package co.cask.cdap.proto;

/**
 * Class containing a program state.
 */
public class ProgramStateMeta {

  private String applicationId;
  private String runnableId;
  private ProgramState status;

  public ProgramStateMeta(String applicationId, String runnableId, ProgramState status) {
    this.applicationId = applicationId;
    this.runnableId = runnableId;
    this.status = status;
  }

  public ProgramState getState() {
    return this.status;
  }
}
