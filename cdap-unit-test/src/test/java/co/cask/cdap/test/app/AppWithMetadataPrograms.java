/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * App which has programs interacting with cdap metadata for tests
 */
public class AppWithMetadataPrograms extends AbstractApplication {
  public static final String APP_NAME = "AppWithMetadataPrograms";
  static final String METADATA_SERVICE_NAME = "MetadataService";
  static final String METADATA_SERVICE_DATASET = "MetadataServiceDataset";
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new Gson();

  @Override
  public void configure() {
    setName(APP_NAME);
    addService(new BasicService(METADATA_SERVICE_NAME, new MetadataHandler()));
  }

  public static final class MetadataHandler extends AbstractHttpServiceHandler {

    /************************************************ GET ************************************************************/
    @GET
    @Path("metadata/{dataset}")
    public void getMetadata(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("dataset") String dataset) {
      Map<MetadataScope, Metadata> metadata = null;
      try {
        metadata = getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
      } catch (Exception e) {
        if (e.getCause() instanceof UnauthorizedException) {
          responder.sendStatus(((UnauthorizedException) e.getCause()).getStatusCode());
        } else if (e.getCause() instanceof UnauthenticatedException) {
          responder.sendStatus(((UnauthenticatedException) e.getCause()).getStatusCode());
        } else if (e.getCause() instanceof BadRequestException) {
          responder.sendStatus(((BadRequestException) e.getCause()).getStatusCode());
        } else {
          responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
        }
      }
      responder.sendJson(HttpResponseStatus.OK.code(), metadata);
    }

    /************************************************* PUT ***********************************************************/
    @PUT
    @Path("metadata/{dataset}/tags")
    public void addTag(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("dataset") String dataset) {
      String tag = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      getContext().addTags(MetadataEntity.ofDataset(getContext().getNamespace(), dataset), tag);
      // wait till change is recorded to store
      try {
        Tasks.waitFor(true, () -> {
          Map<MetadataScope, Metadata> metadata2 =
            getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
          return metadata2.get(MetadataScope.USER).getTags().contains(tag);
        }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
      }
      responder.sendStatus(HttpResponseStatus.OK.code());
    }

    @PUT
    @Path("metadata/{dataset}/properties")
    public void addProperties(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("dataset") String dataset) {
      String body = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      Map<String, String> properties = GSON.fromJson(body, MAP_STRING_STRING_TYPE);
      getContext().addProperties(MetadataEntity.ofDataset(getContext().getNamespace(), dataset), properties);
      // wait till change is recorded to store
      try {
        Tasks.waitFor(true, () -> {
          Map<MetadataScope, Metadata> metadata2 =
            getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
          return metadata2.get(MetadataScope.USER).getProperties().keySet().containsAll(properties.keySet());
        }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
      }
      responder.sendStatus(HttpResponseStatus.OK.code());
    }

    /************************************************ DELETE *********************************************************/
    @DELETE
    @Path("metadata/{dataset}/tags/{tag}")
    public void removeTag(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("dataset") String dataset, @PathParam("tag") String tag) {
      getContext().removeTags(MetadataEntity.ofDataset(getContext().getNamespace(), dataset), tag);
      // wait till change is recorded to store
      try {
        Tasks.waitFor(true, () -> {
          Map<MetadataScope, Metadata> metadata2 =
            getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
          return !metadata2.get(MetadataScope.USER).getTags().contains(tag);
        }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
      }
      responder.sendStatus(HttpResponseStatus.OK.code());
    }

    @DELETE
    @Path("metadata/{dataset}/tags")
    public void removeAllTags(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("dataset") String dataset) {
      getContext().removeTags(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
      // wait till change is recorded to store
      try {
        Tasks.waitFor(true, () -> {
          Map<MetadataScope, Metadata> metadata2 =
            getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
          return metadata2.get(MetadataScope.USER).getTags().isEmpty();
        }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
      }
      responder.sendStatus(HttpResponseStatus.OK.code());
    }

    @DELETE
    @Path("metadata/{dataset}/properties/{key}")
    public void removeProperty(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("dataset") String dataset, @PathParam("key") String key) {
      getContext().removeProperties(MetadataEntity.ofDataset(getContext().getNamespace(), dataset), key);
      // wait till change is recorded to store
      try {
        Tasks.waitFor(true, () -> {
          Map<MetadataScope, Metadata> metadata2 =
            getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
          return !metadata2.get(MetadataScope.USER).getProperties().containsKey(key);
        }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
      }
      responder.sendStatus(HttpResponseStatus.OK.code());
    }

    @DELETE
    @Path("metadata/{dataset}/properties")
    public void removeAllProperties(HttpServiceRequest request, HttpServiceResponder responder,
                                    @PathParam("dataset") String dataset) {
      getContext().removeProperties(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
      // wait till change is recorded to store
      try {
        Tasks.waitFor(true, () -> {
          Map<MetadataScope, Metadata> metadata2 =
            getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
          return metadata2.get(MetadataScope.USER).getProperties().isEmpty();
        }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
      }
      responder.sendStatus(HttpResponseStatus.OK.code());
    }

    @DELETE
    @Path("metadata/{dataset}")
    public void removeMetadata(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("dataset") String dataset) {
      getContext().removeMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
      // wait till change is recorded to store
      try {
        Tasks.waitFor(true, () -> {
          Map<MetadataScope, Metadata> metadata2 =
            getContext().getMetadata(MetadataEntity.ofDataset(getContext().getNamespace(), dataset));
          return metadata2.get(MetadataScope.USER).getProperties().isEmpty() && metadata2.get(MetadataScope.USER)
            .getTags().isEmpty();
        }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
      }
      responder.sendStatus(HttpResponseStatus.OK.code());
    }

    @Override
    protected void configure() {
      createDataset(METADATA_SERVICE_DATASET, KeyValueTable.class);
    }
  }
}
