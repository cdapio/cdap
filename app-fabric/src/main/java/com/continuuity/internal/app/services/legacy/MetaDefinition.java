/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.services.legacy;

/**
 * MetaDefinition is a read-only interface for reading the attributes of the
 * meta section of a flow.
 */
public interface MetaDefinition {

  /**
   * Returns the name of the flow.
   *
   * @return name of the flow.
   */
  public String getName();

  /**
   * Returns the email associated with the flow to which all failure status
   * are reported to.
   *
   * @return email address associated with the flow.
   */
  public String getEmail();

  /**
   * Returns name of the company who developed this flow.
   *
   * @return name of the company.
   */
  public String getCompany();

  /**
   * Returns application name the flow belongs to.
   *
   * @return application the flow belongs to.
   */
  public String getApp();
}
