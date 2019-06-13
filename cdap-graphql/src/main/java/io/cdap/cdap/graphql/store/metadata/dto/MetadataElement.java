/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.store.metadata.dto;

/**
 * DTO object for structuring an element of metadata
 */
public abstract class MetadataElement {

  private final String name;
  private final String scope;

  MetadataElement(String name, String scope) {
    this.name = name;
    this.scope = scope;
  }

  public String getName() {
    return name;
  }

  public String getScope() {
    return scope;
  }

}
