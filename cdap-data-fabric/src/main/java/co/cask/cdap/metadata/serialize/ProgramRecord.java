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

package co.cask.cdap.metadata.serialize;

import java.util.Objects;

/**
 * Class to serialize {@link co.cask.cdap.proto.Id.Program}.
 */
public class ProgramRecord {
  private final String namespace;
  private final String application;
  private final String type;
  private final String id;

  public ProgramRecord(String namespace, String application, String type, String id) {
    this.namespace = namespace;
    this.application = application;
    this.type = type;
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProgramRecord program = (ProgramRecord) o;
    return Objects.equals(namespace, program.namespace) &&
      Objects.equals(application, program.application) &&
      Objects.equals(type, program.type) &&
      Objects.equals(id, program.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, application, type, id);
  }

  @Override
  public String toString() {
    return "ProgramRecord{" +
      "namespace='" + namespace + '\'' +
      ", application='" + application + '\'' +
      ", type='" + type + '\'' +
      ", id='" + id + '\'' +
      '}';
  }
}
