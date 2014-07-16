/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.jetstream.api;

/**
 * Configures InputFlowlet.
 */
public interface InputFlowletConfigurer {

  /**
   * Sets the InputFlowlet Name.
   * @param name Name of the InputFlowlet.
   */
  void setName(String name);

  /**
   * Sets the description of the InputFlowet.
   * @param description Description of the InputFlowlet.
   */
  void setDescription(String description);

  /**
   * Adds a GDAT Input to the InputFlowlet.
   * @param name Name of the Input.
   * @param schema Attach a schema to the Input.
   */
  void addGDATInput(String name, StreamSchema schema);

  /**
   * Adds a GSQL query to the InputFlowlet.
   * @param outputName Name of the GSQL Query (also the name of the output stream).
   * @param gsql GSQL query.
   */
  void addGSQL(String outputName, String gsql);
}
