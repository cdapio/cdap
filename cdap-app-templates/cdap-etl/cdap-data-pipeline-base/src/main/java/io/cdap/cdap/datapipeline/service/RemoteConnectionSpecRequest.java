/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import io.cdap.cdap.etl.proto.connection.Connection;

public class RemoteConnectionSpecRequest {

  private String namespace;
  //original browse request
  private String specRequest;
  private Connection connection;

  RemoteConnectionSpecRequest(String namespace, String specRequest, Connection connection) {
    this.namespace = namespace;
    this.specRequest = specRequest;
    this.connection = connection;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getSpecRequest() {
    return specRequest;
  }

  public Connection getConnection() {
    return connection;
  }
}
