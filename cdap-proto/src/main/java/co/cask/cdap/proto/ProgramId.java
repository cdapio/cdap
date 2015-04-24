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

/**
 * Represents an adapter returned for /adapters/{adapter-id}.
 */
public class ProgramId {
  private final String namespace;
  private final String application;
  private final ProgramType type;
  private final String id;

  public ProgramId(Id.Program program) {
    this(program.getNamespaceId(), program.getApplicationId(), program.getType(), program.getId());
  }

  public ProgramId(String namespace, String application, ProgramType type, String id) {
    this.namespace = namespace;
    this.application = application;
    this.type = type;
    this.id = id;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getApplication() {
    return application;
  }

  public ProgramType getType() {
    return type;
  }

  public String getId() {
    return id;
  }
}
