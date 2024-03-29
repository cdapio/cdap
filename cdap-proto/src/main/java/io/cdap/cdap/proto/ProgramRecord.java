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

package io.cdap.cdap.proto;

/**
 * Represents a program in an HTTP response.
 */
public class ProgramRecord {

  private final ProgramType type;
  private final String app;
  private final String name;
  private final String description;

  public ProgramRecord(ProgramType type, String app, String name, String description) {
    this.type = type;
    this.app = app;
    this.name = name;
    this.description = description;
  }

  public ProgramType getType() {
    return type;
  }

  public String getApp() {
    return app;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }
}
