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

package co.cask.cdap.metadata;

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageService;
import co.cask.cdap.proto.Id;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * HttpHandler for lineage.
 */
@Path(Constants.Gateway.API_VERSION_3)
public class LineageHandler extends AbstractHttpHandler {
  private final LineageService lineageService;

  @Inject
  LineageHandler(LineageService lineageService) {
    this.lineageService = lineageService;
  }

  @POST
  @Path("/namespaces/{namespace-id}/datasets/{dataset-id}/lineage")
  public void datasetLineage(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("dataset-id") String datasetId,
                             @QueryParam("start") @DefaultValue("-1") long start,
                             @QueryParam("end") @DefaultValue("-1") long end,
                             @QueryParam("levels") @DefaultValue("10") int levels) throws Exception {

    checkArguments(start, end, levels);

    Id.DatasetInstance datasetInstance = Id.DatasetInstance.from(namespaceId, datasetId);
    Lineage lineage = lineageService.computeLineage(datasetInstance, start, end, levels);
    responder.sendJson(HttpResponseStatus.OK, lineage);
  }

  private void checkArguments(long start, long end, int levels) throws BadRequestException {
    if (start < 0) {
      throw new BadRequestException(String.format("Invalid start time (%d), should be >= 0.", start));
    }
    if (end < 0) {
      throw new BadRequestException(String.format("Invalid end time (%d), should be >= 0.", end));
    }
    if (start > end) {
      throw new BadRequestException(String.format("start time (%d) should be lesser than end time (%d).", start, end));
    }
    if (levels < 1) {
      throw new BadRequestException(String.format("Invalid levels (%d), should be greater than 0.", levels));
    }
  }
}
