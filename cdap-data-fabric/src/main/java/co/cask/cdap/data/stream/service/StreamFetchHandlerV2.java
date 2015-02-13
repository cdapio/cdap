/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.RESTMigrationUtils;
import co.cask.cdap.common.stream.StreamEventTypeAdapter;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * A HTTP handler for handling getting stream events.
 */
@Path(Constants.Gateway.API_VERSION_2 + "/streams")
public final class StreamFetchHandlerV2 extends AuthenticatedHttpHandler {

  // StreamFetchHandler for V3 APIs,to which calls will be delegated to.
  private final StreamFetchHandler streamFetchHandler;

  @Inject
  public StreamFetchHandlerV2(Authenticator authenticator, StreamFetchHandler streamFetchHandler) {
    super(authenticator);
    this.streamFetchHandler = streamFetchHandler;
  }

  /**
   * Handler for the HTTP API {@code /streams/[stream_name]/events?start=[start_ts]&end=[end_ts]&limit=[event_limit]}
   * <p/>
   * Response with
   * 404 if stream not exists.
   * 204 if no event in the given start/end time range
   * 200 if there is event
   * <p/>
   * Response body is an Json array of StreamEvent object
   *
   * @see StreamEventTypeAdapter for the format of StreamEvent object.
   */
  @GET
  @Path("/{stream}/events")
  public void fetch(HttpRequest request, HttpResponder responder,
                    @PathParam("stream") String stream,
                    @QueryParam("start") long startTime,
                    @QueryParam("end") @DefaultValue("9223372036854775807") long endTime,
                    @QueryParam("limit") @DefaultValue("2147483647") int limit) throws Exception {
    streamFetchHandler.fetch(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                             Constants.DEFAULT_NAMESPACE, stream, startTime, endTime, limit);
  }
}
