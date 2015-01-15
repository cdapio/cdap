/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.app.program;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import org.apache.twill.filesystem.Location;

/**
 * Abstraction of a executable program.
 */
public interface Program {

  /**
   * Returns the name of the main class in the program.
   */
  String getMainClassName();

  /**
   * Loads and returns the main class of the program
   * @throws ClassNotFoundException If fails to load the class.
   */
  <T> Class<T> getMainClass() throws ClassNotFoundException;

  /**
   * Returns the program type.
   */
  ProgramType getType();

  /**
   * Returns the program ID.
   */
  Id.Program getId();

  /**
   * Returns name of the program.
   */
  String getName();

  /**
   * Returns the ID of the namespace that this program belongs to.
   */
  String getNamespaceId();

  /**
   * Returns the application ID that this program belongs to.
   */
  String getApplicationId();

  /**
   * Returns the complete application specification that contains this program.
   */
  ApplicationSpecification getSpecification();

  /**
   * Returns the location of the jar file of this program.
   */
  Location getJarLocation();

  /**
   * Returns the class loader for loading classes inside this program.
   */
  ClassLoader getClassLoader();
}
