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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.AbstractBodyConsumer;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetModuleConflictException;
import co.cask.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyConsumer;
import co.cask.http.HandlerContext;
import co.cask.http.HttpResponder;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles dataset type management calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class DatasetTypeHandler extends AbstractHttpHandler {
  public static final String HEADER_CLASS_NAME = "X-Class-Name";

  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeHandler.class);

  private final DatasetTypeManager manager;
  private final LocationFactory locationFactory;
  private final CConfiguration cConf;

  @Inject
  public DatasetTypeHandler(DatasetTypeManager manager, LocationFactory locationFactory, CConfiguration conf) {
    this.manager = manager;
    this.locationFactory = locationFactory;
    this.cConf = conf;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting DatasetTypeHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping DatasetTypeHandler");
  }

  @GET
  @Path("/data/modules")
  public void listModules(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId) {
    // Sorting by name for convenience
    List<DatasetModuleMeta> list = Lists.newArrayList(manager.getModules(Id.Namespace.from(namespaceId)));
    Collections.sort(list, new Comparator<DatasetModuleMeta>() {
      @Override
      public int compare(DatasetModuleMeta o1, DatasetModuleMeta o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    responder.sendJson(HttpResponseStatus.OK, list);
  }

  @DELETE
  @Path("/data/modules")
  public void deleteModules(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId) {
    if (Constants.SYSTEM_NAMESPACE.equals(namespaceId)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, "Cannot delete system dataset modules.");
      return;
    }
    try {
      manager.deleteModules(Id.Namespace.from(namespaceId));
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (DatasetModuleConflictException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
    }
  }

  @PUT
  @Path("/data/modules/{name}")
  public BodyConsumer addModule(HttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") String namespaceId, @PathParam("name") final String name,
                                @HeaderParam(HEADER_CLASS_NAME) final String className) throws IOException {
    if (Constants.SYSTEM_NAMESPACE.equals(namespaceId)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, "Cannot deploy modules to system namespace.");
      return null;
    }

    // verify namespace directory exists
    final Location namespaceHomeLocation = locationFactory.create(namespaceId);
    if (!namespaceHomeLocation.exists()) {
      String msg = String.format("Home directory %s for namespace %s not found",
                                 namespaceHomeLocation.toURI().getPath(), namespaceId);
      LOG.error(msg);
      responder.sendString(HttpResponseStatus.NOT_FOUND, msg);
      return null;
    }

    // Store uploaded content to a local temp file
    String tempBase = String.format("%s/%s", cConf.get(Constants.CFG_LOCAL_DATA_DIR), namespaceId);
    File tempDir = new File(tempBase, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException("Could not create temporary directory at: " + tempDir);
    }

    final Id.DatasetModule datasetModuleId = Id.DatasetModule.from(namespaceId, name);

    return new AbstractBodyConsumer(File.createTempFile("dataset-", ".jar", tempDir)) {
      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) throws Exception {
        if (className == null) {
          // We have to delay until body upload is completed due to the fact that not all client is
          // requesting with "Expect: 100-continue" header and the client library we have cannot handle
          // connection close, and yet be able to read response reliably.
          // In longer term we should fix the client, as well as the netty-http server. However, since
          // this handler will be gone in near future, it's ok to have this workaround.
          responder.sendString(HttpResponseStatus.BAD_REQUEST, "Required header 'class-name' is absent.");
          return;
        }

        LOG.info("Adding module {}, class name: {}", datasetModuleId, className);

        String dataFabricDir = cConf.get(Constants.Dataset.Manager.OUTPUT_DIR);
        Location archiveDir = namespaceHomeLocation.append(dataFabricDir).append(name)
          .append(Constants.ARCHIVE_DIR);
        String archiveName = name + ".jar";
        Location archive = archiveDir.append(archiveName);

        // Copy uploaded content to a temporary location
        Location tmpLocation = archive.getTempFile(".tmp");
        try {
          conflictIfModuleExists(datasetModuleId);

          Locations.mkdirsIfNotExists(archiveDir);

          LOG.debug("Copy from {} to {}", uploadedFile, tmpLocation.toURI());
          Files.copy(uploadedFile, Locations.newOutputSupplier(tmpLocation));

          // Check if the module exists one more time to minimize the window of possible conflict
          conflictIfModuleExists(datasetModuleId);

          // Finally, move archive to final location
          LOG.debug("Storing module {} jar at {}", datasetModuleId, archive.toURI());
          if (tmpLocation.renameTo(archive) == null) {
            throw new IOException(String.format("Could not move archive from location: %s, to location: %s",
                                                tmpLocation.toURI(), archive.toURI()));
          }

          manager.addModule(datasetModuleId, className, archive);
          // todo: response with DatasetModuleMeta of just added module (and log this info)
          LOG.info("Added module {}", datasetModuleId);
          responder.sendStatus(HttpResponseStatus.OK);
        } catch (Exception e) {
          // In case copy to temporary file failed, or rename failed
          try {
            tmpLocation.delete();
          } catch (IOException ex) {
            LOG.warn("Failed to cleanup temporary location {}", tmpLocation.toURI());
          }
          if (e instanceof DatasetModuleConflictException) {
            responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
          } else {
            LOG.error("Failed to add module {}", name, e);
            throw e;
          }
        }
      }
    };
  }

  @DELETE
  @Path("/data/modules/{name}")
  public void deleteModule(HttpRequest request, HttpResponder responder,
                           @PathParam("namespace-id") String namespaceId, @PathParam("name") String name) {
    if (Constants.SYSTEM_NAMESPACE.equals(namespaceId)) {
      responder.sendString(HttpResponseStatus.FORBIDDEN,
                           String.format("Cannot delete modules from '%s' namespace.", namespaceId));
      return;
    }
    boolean deleted;
    try {
      deleted = manager.deleteModule(Id.DatasetModule.from(namespaceId, name));
    } catch (DatasetModuleConflictException e) {
      responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
      return;
    }

    if (!deleted) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }

    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/data/modules/{name}")
  public void getModuleInfo(HttpRequest request, HttpResponder responder,
                            @PathParam("namespace-id") String namespaceId, @PathParam("name") String name) {
    DatasetModuleMeta moduleMeta = manager.getModule(Id.DatasetModule.from(namespaceId, name));
    if (moduleMeta == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      responder.sendJson(HttpResponseStatus.OK, moduleMeta);
    }
  }

  @GET
  @Path("/data/types")
  public void listTypes(HttpRequest request, HttpResponder responder,
                        @PathParam("namespace-id") String namespaceId) {
    // Sorting by name for convenience
    List<DatasetTypeMeta> list = Lists.newArrayList(manager.getTypes(Id.Namespace.from(namespaceId)));
    Collections.sort(list, new Comparator<DatasetTypeMeta>() {
      @Override
      public int compare(DatasetTypeMeta o1, DatasetTypeMeta o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    responder.sendJson(HttpResponseStatus.OK, list);
  }

  @GET
  @Path("/data/types/{name}")
  public void getTypeInfo(HttpRequest request, HttpResponder responder,
                          @PathParam("namespace-id") String namespaceId, @PathParam("name") String name) {

    DatasetTypeMeta typeMeta = manager.getTypeInfo(Id.DatasetType.from(namespaceId, name));
    if (typeMeta == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      responder.sendJson(HttpResponseStatus.OK, typeMeta);
    }
  }

  /**
   * Checks if the given module name already exists.
   *
   * @param datasetModuleId {@link Id.DatasetModule} of the module to check
   * @throws DatasetModuleConflictException if the module exists
   */
  private void conflictIfModuleExists(Id.DatasetModule datasetModuleId) throws DatasetModuleConflictException {
    DatasetModuleMeta existing = manager.getModule(datasetModuleId);
    if (existing != null) {
      String message = String.format("Cannot add module %s: module with same name already exists: %s",
                                     datasetModuleId, existing);
      throw new DatasetModuleConflictException(message);
    }
  }
}
