/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.app.runtime.workflow.WorkflowStatus;
import co.cask.cdap.internal.app.WorkflowActionSpecificationCodec;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Supplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * A HttpHandler for handling Workflow REST API.
 */
public final class WorkflowServiceHandler extends AbstractHttpHandler {

  private static final Gson GSON = new GsonBuilder()
                                    .registerTypeAdapter(WorkflowActionSpecification.class,
                                                         new WorkflowActionSpecificationCodec())
                                    .create();

  private final Supplier<Map<String, WorkflowStatus>> statusSupplier;

  WorkflowServiceHandler(Supplier<Map<String, WorkflowStatus>> statusSupplier) {
    this.statusSupplier = statusSupplier;
  }

  /**
   * Provides response to {@code /status} call to gives the latest status of this workflow.
   */
  @GET
  @Path("/status")
  public void handleStatus(HttpRequest request, HttpResponder responder) {
    Type mapType = new TypeToken<Map<String, WorkflowStatus>>() { }.getType();
    responder.sendJson(HttpResponseStatus.OK, statusSupplier.get(), mapType, GSON);
  }
}
