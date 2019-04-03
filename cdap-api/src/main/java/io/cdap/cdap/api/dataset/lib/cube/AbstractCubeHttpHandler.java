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
package co.cask.cdap.api.dataset.lib.cube;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collection;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * A basic implementation of {@link co.cask.cdap.api.service.http.HttpServiceHandler} that provides endpoints to
 * explore and execute queries in {@link Cube} dataset.
 * <p/>
 * Subclasses must implement {@link co.cask.cdap.api.dataset.lib.cube.AbstractCubeHttpHandler#getCube()} that returns
 * {@link Cube} dataset.
 */
@Beta
public abstract class AbstractCubeHttpHandler extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractCubeHttpHandler.class);

  private static final Type CUBE_FACT_COLLECTION = new TypeToken<Collection<CubeFact>>() { }.getType();
  private static final Gson GSON = new Gson();

  /**
   * @return {@link Cube} dataset.
   */
  protected abstract Cube getCube();

  /**
   * Adds facts to a {@link Cube} as defined by {@link Cube#add(java.util.Collection)}.
   */
  @Path("add")
  @POST
  public void addFact(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      String body = Bytes.toString(request.getContent());
      Collection<CubeFact> facts = GSON.fromJson(body, CUBE_FACT_COLLECTION);
      getCube().add(facts);
      responder.sendStatus(200);
    } catch (Throwable th) {
      LOG.error("Error while executing request", th);
      responder.sendError(500, th.getMessage());
    }
  }

  /**
   * Searches dimension values in a {@link Cube} as defined by
   * {@link Cube#findDimensionValues(CubeExploreQuery)}.
   */
  @Path("searchDimensionValue")
  @POST
  public void searchDimensionValue(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      String body = Bytes.toString(request.getContent());
      CubeExploreQuery query = GSON.fromJson(body, CubeExploreQuery.class);
      Collection<DimensionValue> result = getCube().findDimensionValues(query);
      responder.sendJson(result);
    } catch (Throwable th) {
      LOG.error("Error while executing request", th);
      responder.sendError(500, th.getMessage());
    }
  }

  /**
   * Searches measurements in a {@link Cube} as defined by {@link Cube#findMeasureNames(CubeExploreQuery)}.
   */
  @Path("searchMeasure")
  @POST
  public void searchMeasure(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      String body = Bytes.toString(request.getContent());
      CubeExploreQuery query = GSON.fromJson(body, CubeExploreQuery.class);
      Collection<String> result = getCube().findMeasureNames(query);
      responder.sendJson(result);
    } catch (Throwable th) {
      LOG.error("Error while executing request", th);
      responder.sendError(500, th.getMessage());
    }
  }

  /**
   * Queries data in a {@link Cube} as defined by {@link Cube#query(CubeQuery)}.
   */
  @Path("query")
  @POST
  public void query(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      String body = Bytes.toString(request.getContent());
      CubeQuery query = GSON.fromJson(body, CubeQuery.class);
      Collection<TimeSeries> result = getCube().query(query);
      responder.sendJson(result);
    } catch (Throwable th) {
      LOG.error("Error while executing request", th);
      responder.sendError(500, th.getMessage());
    }
  }
}
