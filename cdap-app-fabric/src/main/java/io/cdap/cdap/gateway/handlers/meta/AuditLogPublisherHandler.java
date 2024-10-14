/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.meta;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.auditlogging.AuditLogPublisherService;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * An HTTP Handler that runs inside the master and communicates with
 * {@link AuditLogPublisherService} to send the audit logs received to publish.
 *
 */
@Path(AbstractRemoteSystemOpsHandler.VERSION + "/execute")
public class AuditLogPublisherHandler extends AbstractRemoteSystemOpsHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLogPublisherHandler.class);
  private final AuditLogPublisherService auditLogPublisherService;

  @Inject
  AuditLogPublisherHandler(AuditLogPublisherService auditLogPublisherService) {
    this.auditLogPublisherService = auditLogPublisherService;
  }

  @POST
  @Path("/publish")
  public void publish(FullHttpRequest request, HttpResponder responder) throws Exception {
    AuditLogContext auditLogContext = new Gson().fromJson(
        request.content().toString(StandardCharsets.UTF_8),
        AuditLogContext.class);
    LOG.debug("SANKET in handler publish  for {}", auditLogContext);
    auditLogPublisherService.addAuditContexts(new ArrayDeque<>());
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/publishbatch")
  public void publishBatch(FullHttpRequest request, HttpResponder responder) throws Exception {
    LOG.debug("SANKET in handler publishbatch  for {}", request.content().toString(StandardCharsets.UTF_8));
    Type queueType = new TypeToken<LinkedBlockingDeque<AuditLogContext>>(){}.getType();
    Queue<AuditLogContext> deserializedQueue =
      new Gson().fromJson(request.content().toString(StandardCharsets.UTF_8), queueType);
    auditLogPublisherService.addAuditContexts(deserializedQueue);
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
