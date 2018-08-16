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
package co.cask.cdap.api.annotation;

/**
 * <p>Defines the different capability options for {@link Capability} to specify.
 * These are system defined capabilities.</p>
 *
 * <p>If {@link Capability} does not specifically define any of the capability from {@link Capabilities} then
 * default system capability will be assumed for the {@link Plugin} on which the {@link Capability} is defined.</p>
 *
 * <p>Default system capability is also applied if a plugin does not specify any {@link Capability} at all.</p>
 *
 * <p>According to default system capability a plugin is considered to be capable of all the capabilities defined in
 * {@link Capabilities}.</p>
 */
public final class Capabilities {
  /**
   * Defines capability of running natively
   */
  public static final String NATIVE = "native";

  /**
   * Defines capability of running remotely
   */
  public static final String REMOTE = "remote"; //
}
