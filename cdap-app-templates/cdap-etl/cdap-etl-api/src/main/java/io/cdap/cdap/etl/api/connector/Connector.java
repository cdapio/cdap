/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.api.connector;

import io.cdap.cdap.etl.api.validation.ValidationException;
import java.io.Closeable;
import java.io.IOException;

/**
 * A connector is a plugin which is able to explore and sample an external resource
 */
public interface Connector extends Closeable {

  String PLUGIN_TYPE = "connector";

  /**
   * Configure this connector, for example, the database connector will need to load the jdbc
   * driver. This method is guaranteed to be called before any other method in this class.
   */
  default void configure(ConnectorConfigurer configurer) throws IOException {
    // no-op
  }

  /**
   * Test if the connector is able to connect to the resource
   *
   * @param context context for the connector
   * @throws ValidationException if the connector is not able to connect to the source
   */
  void test(ConnectorContext context) throws ValidationException;

  /**
   * Browse the resources on the given request. The browse request expects a path to represent the
   * hierarchy of the resource. The path is expected to be separated by '/'. If the given path is
   * not browsable, the result will contain the information on the current path. For example, for a
   * file based connector, the path will just be the file/directory path for a database connector,
   * the path can be {database}/{table}
   *
   * @param context context for the connector
   * @param request the browse request
   */
  BrowseDetail browse(ConnectorContext context, BrowseRequest request) throws IOException;

  /**
   * Generate spec based on the given path, the spec should contain all the properties associated
   * with the path. For example, for bigquery, this can be a map containing "datasets": {dataset},
   * "table": {table}
   *
   * @param context context for the connector
   * @param path the path of the entity
   * @return the spec which contains all the properties associated with the path
   */
  ConnectorSpec generateSpec(ConnectorContext context, ConnectorSpecRequest path)
      throws IOException;

  @Override
  default void close() throws IOException {
    // no-op
  }
}
