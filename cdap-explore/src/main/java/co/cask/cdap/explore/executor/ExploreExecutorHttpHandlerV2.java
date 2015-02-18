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

package co.cask.cdap.explore.executor;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.RESTMigrationUtils;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler that implements internal V2 explore APIs.
 */
@Path(Constants.Gateway.API_VERSION_2 + "/data/explore")
public class ExploreExecutorHttpHandlerV2 extends AbstractHttpHandler {
  private final ExploreExecutorHttpHandler exploreExecutorHttpHandler;

  @Inject
  public ExploreExecutorHttpHandlerV2(ExploreExecutorHttpHandler exploreExecutorHttpHandler) {
    this.exploreExecutorHttpHandler = exploreExecutorHttpHandler;
  }

  @POST
  @Path("streams/{stream}/enable")
  public void enableStream(HttpRequest request, HttpResponder responder, @PathParam("stream") String streamName) {
    exploreExecutorHttpHandler.enableStream(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                            Constants.DEFAULT_NAMESPACE, streamName);
  }

  @POST
  @Path("streams/{stream}/disable")
  public void disableStream(HttpRequest request, HttpResponder responder, @PathParam("stream") String streamName) {
    exploreExecutorHttpHandler.disableStream(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                             Constants.DEFAULT_NAMESPACE, streamName);
  }

  @POST
  @Path("/datasets/{dataset}/enable")
  public void enableDataset(HttpRequest request, HttpResponder responder, @PathParam("dataset") String datasetName) {
    exploreExecutorHttpHandler.enableDataset(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                             Constants.DEFAULT_NAMESPACE, datasetName);
  }

  @POST
  @Path("/datasets/{dataset}/disable")
  public void disableDataset(HttpRequest request, HttpResponder responder, @PathParam("dataset") String datasetName) {
    exploreExecutorHttpHandler.disableDataset(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                              Constants.DEFAULT_NAMESPACE, datasetName);
  }

  @POST
  @Path("/datasets/{dataset}/partitions")
  public void addPartition(HttpRequest request, HttpResponder responder,
                           @PathParam("dataset") String datasetName) {
    exploreExecutorHttpHandler.addPartition(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                            Constants.DEFAULT_NAMESPACE, datasetName);
  }

  @POST
  @Path("/datasets/{dataset}/deletePartition")
  public void dropPartition(HttpRequest request, HttpResponder responder, @PathParam("dataset") String datasetName) {
    exploreExecutorHttpHandler.dropPartition(RESTMigrationUtils.rewriteV2RequestToV3(request), responder,
                                             Constants.DEFAULT_NAMESPACE, datasetName);
  }
}
