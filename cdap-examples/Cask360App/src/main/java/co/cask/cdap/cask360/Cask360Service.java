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
package co.cask.cdap.cask360;

import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Entity;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Table;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Cask360 CDAP Service.
 * <p>
 * Includes a single endpoint at <b>/entity/{id}</b>
 * <p>
 * Endpoint supports GET operations to retrieve entities by ID and PUT
 * operations to store/update entities by ID.
 */
public class Cask360Service extends AbstractService {
  private static final Logger LOG = LoggerFactory.getLogger(Cask360Service.class);

  @Property
  private String name;

  @Property
  private String desc;

  @Property
  private String table;

  public Cask360Service(String name, String desc, String table) {
    this.name = name;
    this.desc = desc;
    this.table = table;
  }

  @Override
  protected void configure() {
    setName(name);
    setDescription(desc);
    addHandler(new Cask360ServiceHandler(table));
  }

  /**
   * Cask360 Service Handler for reading/writing entities by ID.
   */
  public static final class Cask360ServiceHandler extends AbstractHttpServiceHandler {

    private static final Gson gson = Cask360Entity.getGson();

    @Property
    private String tableName;

    private Cask360Table table;

    public Cask360ServiceHandler(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public void initialize(HttpServiceContext context) throws Exception {
      super.initialize(context);
      table = context.getDataset(tableName);
    }

    @Path("entity/{id}")
    @GET
    public void read(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("id") String id) {
      Cask360Entity entity = table.read(id);
      if ((entity == null) || entity.isEmpty()) {
        responder.sendJson(new Cask360ServiceResponse(id, "Entity not found"));
        return;
      }
      LOG.info("\n\n\n\nResponding with: " + entity.toString());
      responder.sendJson(200, new Cask360ServiceResponse(entity), Cask360ServiceResponse.class, gson);
    }

    @Path("entity/{id}")
    @PUT
    public void write(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("id") String id) {
      String data = Bytes.toString(request.getContent());
      Cask360Entity entity = Cask360Entity.fromString(data);
      if ((entity == null) || entity.isEmpty()) {
        responder.sendJson(new Cask360ServiceResponse(id, "Invalid entity data"));
        return;
      }
      table.write(entity);
      responder.sendJson(200, new Cask360ServiceResponse(entity), Cask360ServiceResponse.class, gson);
    }
  }

  /**
   * JSON Response for {@link Cask360ServiceHandler}.
   */
  public static class Cask360ServiceResponse {

    /** Error flag. 0 for success, 1 for failure. */
    int error;

    /** Error message (null on success). */
    String msg;

    /** Entity ID (always specified). */
    String id;

    /** Entity (may be null). */
    Cask360Entity entity;

    /**
     * Constructs an error response for the specified ID and error message.
     *
     * @param id
     *          entity id
     * @param msg
     *          error message
     */
    Cask360ServiceResponse(String id, String msg) {
      this.error = 1;
      this.msg = msg;
      this.id = id;
    }

    /**
     * Constructs a successful response and includes the entity in the response.
     *
     * @param entity
     *          entity in response
     */
    Cask360ServiceResponse(Cask360Entity entity) {
      this.error = 0;
      this.entity = entity;
      this.id = entity.getID();
    }

    /**
     * Constructs a successful response for the specified ID.
     *
     * @param id
     *          entity id
     */
    Cask360ServiceResponse(String id) {
      this.error = 0;
      this.id = id;
    }

    /**
     * Checks if the request returned an error.
     *
     * @return true if there was an error, false if there was no error
     */
    public boolean isError() {
      return this.error == 1;
    }

    /**
     * Checks if the request was successful.
     *
     * @return true if there was no error, false if there was an error
     */
    public boolean isSuccess() {
      return this.error == 0;
    }
  }
}
