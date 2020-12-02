/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Class with fields for requesting Programs that should be started for a capability
 */
public class SystemProgram {

  private final String namespace;
  private final String application;
  private final String type;
  private final String name;
  private final String version;
  private final Map<String, String> args;

  public SystemProgram(String namespace, String application, String type, String name, @Nullable String version,
                       @Nullable Map<String, String> args) {
    this.namespace = namespace;
    this.application = application;
    this.type = type;
    this.name = name;
    this.version = version;
    this.args = args == null ? Collections.emptyMap() : args;
  }

  /**
   * @return namespace {@link String}
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * @return application {@link String}
   */
  public String getApplication() {
    return application;
  }

  /**
   * @return type {@link String} of the program
   */
  public String getType() {
    return type;
  }

  /**
   * @return name {@link String} of the program
   */
  public String getName() {
    return name;
  }

  /**
   * @return version {@link String}, could be nullable
   */
  @Nullable
  public String getVersion() {
    return version;
  }

  /**
   * @return {@link Map<String, String>} of runtime arguments
   */
  public Map<String, String> getArgs() {
    if (args == null) {
      return Collections.emptyMap();
    }
    return args;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    SystemProgram program = (SystemProgram) other;
    return Objects.equals(namespace, program.namespace) &&
      Objects.equals(application, program.application) &&
      Objects.equals(type, program.type) &&
      Objects.equals(name, program.name) &&
      Objects.equals(version, program.version) &&
      Objects.equals(args, program.args);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, application, type, name, version, args);
  }
}
