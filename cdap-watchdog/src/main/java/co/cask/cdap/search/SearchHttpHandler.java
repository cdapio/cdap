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

package co.cask.cdap.search;

import co.cask.cdap.common.conf.Constants;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import org.elasticsearch.client.Client;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;


/**
 * Handler that uses {@link Client} to perform operations on elastic search
 */
@Path(Constants.Gateway.API_VERSION_3 + "/search")
public class SearchHttpHandler extends AbstractHttpHandler {

  private final Client elasticClient;
  public SearchHttpHandler(Client client) {
    this.elasticClient = client;
  }

  @POST
  @Path("/index")
  public void search(HttpRequest request, HttpResponder responder) throws Exception {

  }

  @GET
  @Path("/index")
  public void get(HttpRequest request, HttpResponder responder) throws Exception {
    responder.sendJson(HttpResponseStatus.OK, "Sending Mock Index");
  }

}
