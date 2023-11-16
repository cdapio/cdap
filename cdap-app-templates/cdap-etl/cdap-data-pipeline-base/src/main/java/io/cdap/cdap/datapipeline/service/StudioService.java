/*
 * Copyright © 2019-2021 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.service.AbstractSystemService;
import io.cdap.cdap.api.service.SystemServiceContext;
import io.cdap.cdap.datapipeline.connection.ConnectionStore;
import io.cdap.cdap.datapipeline.draft.DraftStore;
import io.cdap.cdap.datapipeline.oauth.OAuthStore;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.proto.connection.Connection;
import io.cdap.cdap.etl.proto.connection.ConnectionConflictException;
import io.cdap.cdap.etl.proto.connection.ConnectionId;
import io.cdap.cdap.etl.proto.connection.PreconfiguredConnectionCreationRequest;
import io.cdap.cdap.etl.proto.v2.ConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * Service that handles pipeline studio operations, like validation and schema propagation.
 */
public class StudioService extends AbstractSystemService {

  private static final Logger LOG = LoggerFactory.getLogger(StudioService.class);
  private static final String CONNECTION_TYPE_CONFIG = "connectionTypeConfig";
  private static final Gson GSON = new Gson();

  private ConnectionConfig connectionConfig;

  public StudioService(@Nullable ConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
  }

  @Override
  protected void configure() {
    setName(Constants.STUDIO_SERVICE_NAME);
    setDescription(
        "### Handles pipeline studio operations, like validation, connections and schema propagation.");
    addHandler(new ValidationHandler());
    addHandler(new DraftHandler());
    addHandler(new ConnectionHandler(connectionConfig));
    addHandler(new OAuthHandler());
    createTable(DraftStore.TABLE_SPEC);
    createTable(OAuthStore.TABLE_SPEC);
    createTable(ConnectionStore.CONNECTION_TABLE_SPEC);
    setProperties(Collections.singletonMap(CONNECTION_TYPE_CONFIG, GSON.toJson(connectionConfig)));
  }

  @Override
  public void initialize(SystemServiceContext context) throws Exception {
    String connConfig = context.getSpecification().getProperty(CONNECTION_TYPE_CONFIG);
    connectionConfig = GSON.fromJson(connConfig, ConnectionConfig.class);
    // only do the connection creation on first instance to avoid transaction conflict
    if (context.getInstanceId() != 0 || connectionConfig == null
        || connectionConfig.getConnections().isEmpty()) {
      return;
    }

    try {
      createPreconfiguredConnections(context);
    } catch (Exception e) {
      // we don't want the service fail to start
      LOG.error("Error creating the preconfigured connections");
    }
  }

  private void createPreconfiguredConnections(SystemServiceContext context) throws IOException {
    ConnectionStore connectionStore = new ConnectionStore(context);
    for (PreconfiguredConnectionCreationRequest creationRequest : connectionConfig.getConnections()) {
      if (creationRequest.getName() == null || creationRequest.getNamespace() == null) {
        continue;
      }

      NamespaceSummary namespaceSummary = context.getAdmin()
          .getNamespaceSummary(creationRequest.getNamespace());
      if (namespaceSummary == null) {
        LOG.warn("Namespace {} does not exist, skipping creating connection {}",
            creationRequest.getNamespace(),
            creationRequest.getName());
      }

      ConnectionId connectionId = new ConnectionId(namespaceSummary, creationRequest.getName());
      long now = System.currentTimeMillis();
      Connection connectionInfo = new Connection(
          creationRequest.getName(), connectionId.getConnectionId(),
          creationRequest.getPlugin().getName(),
          creationRequest.getDescription(), true,
          creationRequest.getName().equals(connectionConfig.getDefaultConnection()) ? true : false,
          now, now, creationRequest.getPlugin());
      try {
        connectionStore.saveConnection(connectionId, connectionInfo, false);
      } catch (ConnectionConflictException e) {
        // expected if the connection is already created
      }
    }
  }
}
