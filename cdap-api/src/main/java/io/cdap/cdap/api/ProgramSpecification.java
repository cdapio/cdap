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
package io.cdap.cdap.api;

import io.cdap.cdap.api.plugin.Plugin;
import java.util.Map;

/**
 * This interface provides for getting the name, class name, and description specifications of any
 * type of program.
 */
public interface ProgramSpecification {

  /**
   * @return Class name of the program.
   */
  String getClassName();

  /**
   * @return Name of the program.
   */
  String getName();

  /**
   * @return Description of the program.
   */
  String getDescription();

  /**
   * @return {@link Plugin} informations if there are any plugins associated with this program else
   *     returns an empty map
   */
  Map<String, Plugin> getPlugins();
}
