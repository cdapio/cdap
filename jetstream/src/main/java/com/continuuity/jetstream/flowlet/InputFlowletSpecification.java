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

package com.continuuity.jetstream.flowlet;

import com.continuuity.jetstream.api.StreamSchema;

import java.util.Map;

/**
 * Specification of a InputFlowlet.
 */
public interface InputFlowletSpecification {

  /**
   * Get the Name of the Input Flowlet.
   * @return Name of the Input Flowlet.
   */
  String getName();

  /**
   * Get the Description of the Input Flowlet.
   * @return Description of the Input Flowlet.
   */
  String getDescription();

  /**
   * Get the GDATInput Schema.
   * @return Map of Input Name and the associated {@link com.continuuity.jetstream.api.StreamSchema}.
   */
  Map<String, StreamSchema> getGDATInputSchema();

  /**
   * Get the GSQL queries.
   * @return Map of Name of Query Outputs and the corresponding GSQL queries.
   */
  Map<String, String> getGSQL();

}
