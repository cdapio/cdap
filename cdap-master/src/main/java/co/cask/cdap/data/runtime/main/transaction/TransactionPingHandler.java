/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.data.runtime.main.transaction;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.PingHandler;
import co.cask.http.HttpResponder;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tephra.TransactionSystemClient;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handles ping requests for Transaction service.
 * Similar to PingHandler, but overriding the status endpoint, to also check transaction manager status.
 */
public class TransactionPingHandler extends PingHandler {
  private static final JsonObject NOT_OK_JSON;
  static {
    NOT_OK_JSON = new JsonObject();
    NOT_OK_JSON.addProperty("status", Constants.Monitor.STATUS_NOTOK);
  }

  private final TransactionSystemClient transactionSystemClient;

  @Inject
  public TransactionPingHandler(TransactionSystemClient transactionSystemClient) {
    this.transactionSystemClient = transactionSystemClient;
  }

  @Path(Constants.Gateway.API_VERSION_3 + "/system/services/{service-name}/status")
  @GET
  public void status(HttpRequest request, HttpResponder responder) {
    // ignore the service-name, since we dont need it. its only used for routing
    responder.sendJson(HttpResponseStatus.OK,
                       Constants.Monitor.STATUS_OK.equals(transactionSystemClient.status())
                         ? OK_JSON.toString() : NOT_OK_JSON.toString());
  }
}
