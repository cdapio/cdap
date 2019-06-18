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

package io.cdap.cdap.graphql.store.namespace.schema;

/**
 * Helper class with a collection of fields relevant to namespaces that are used in the server
 */
public class NamespaceFields {

  public static final String NAMESPACES = "namespaces";
  public static final String NAME = "name";
  public static final String DESCRIPTION = "description";
  public static final String GENERATION = "generation";
  public static final String NAMESPACE = "namespace";

  private NamespaceFields() {
    throw new UnsupportedOperationException("Helper class should not be instantiated");
  }

}
