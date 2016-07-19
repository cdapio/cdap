/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers.meta;

import co.cask.cdap.common.internal.remote.MethodArgument;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.proto.Id;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Iterator;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * The {@link co.cask.http.HttpHandler} for handling REST calls to UsageDataset.
 */
@Path(AbstractRemoteSystemOpsHandler.VERSION + "/execute")
public class RemoteUsageRegistryHandler extends AbstractRemoteSystemOpsHandler {

  private final UsageRegistry usageRegistry;

  @Inject
  RemoteUsageRegistryHandler(UsageRegistry usageRegistry) {
    this.usageRegistry = usageRegistry;
  }

  @POST
  @Path("/registerDataset")
  public void registerDataset(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);

    Id.Program programId = deserializeNext(arguments);
    Id.DatasetInstance datasetInstance = deserializeNext(arguments);
    usageRegistry.register(programId, datasetInstance);

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/registerStream")
  public void registerStream(HttpRequest request, HttpResponder responder) throws Exception {
    Iterator<MethodArgument> arguments = parseArguments(request);

    Id.Program programId = deserializeNext(arguments);
    Id.Stream streamId = deserializeNext(arguments);
    usageRegistry.register(programId, streamId);

    responder.sendStatus(HttpResponseStatus.OK);
  }
}
