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

package com.continuuity.explore.utils;

import com.google.common.base.Objects;

import java.util.List;

/**
 * Class to represent a JSON object passed as an argument to the getTables metadata HTTP endpoint.
 */
public final class TablesArgs {
  private final String catalog;
  private final String schemaPattern;
  private final String tableNamePattern;
  private final List<String> tableTypes;

  public TablesArgs(String catalog, String schemaNamePattern, String tableNamePattern, List<String> tableTypes) {
    this.catalog = catalog;
    this.schemaPattern = schemaNamePattern;
    this.tableNamePattern = tableNamePattern;
    this.tableTypes = tableTypes;
  }

  public String getTableNamePattern() {
    return tableNamePattern;
  }

  public List<String> getTableTypes() {
    return tableTypes;
  }

  public String getSchemaPattern() {
    return schemaPattern;
  }

  public String getCatalog() {
    return catalog;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("catalog", catalog)
      .add("schemaPattern", schemaPattern)
      .add("tableNamePattern", tableNamePattern)
      .add("tableTypes", tableTypes)
      .toString();
  }
}
