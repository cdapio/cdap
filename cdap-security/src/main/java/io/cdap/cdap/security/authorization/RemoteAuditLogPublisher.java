/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import io.cdap.cdap.api.auditlogging.AuditLogPublisher;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.internal.remote.RemoteOpsClient;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.PermissionAdapterFactory;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

/**
 *
 */
public class RemoteAuditLogPublisher extends RemoteOpsClient implements AuditLogPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteAuditLogPublisher.class);
  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
      .registerTypeAdapterFactory(new PermissionAdapterFactory())
      .create();
  @Inject
  RemoteAuditLogPublisher(RemoteClientFactory remoteClientFactory) {
    super(remoteClientFactory, Constants.Service.APP_FABRIC_HTTP);
  }

  public void publish(Queue<AuditLogContext> auditLogContexts)
      throws UnauthorizedException {
    LOG.trace("SANKET : Making pushlish to {}", auditLogContexts);
    executeRequest("publishbatch", auditLogContexts);
    LOG.trace("SANKET : Success pushlish to {}", auditLogContexts);
  }
}
