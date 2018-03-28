/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report;

/**
 * run id fields
 */
public class ProgramRunIdFields {
  private final String application;
  private final String version;
  private final String type;
  private final String program;
  private final String run;
  private final String namespace;

  ProgramRunIdFields(String application, String version, String type, String program, String run,
                     String namespace) {
    this.application = application;
    this.version = version;
    this.type = type;
    this.program = program;
    this.run = run;
    this.namespace = namespace;
  }
}
