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
import co.cask.http.AbstractHttpHandler;
import co.cask.http.BodyConsumer;
import co.cask.http.HandlerContext;
import co.cask.http.HttpResponder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpHeaders;
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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles dataset type management calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.GATEWAY_VERSION)
public class DatasetTypeHandler extends AbstractHttpHandler {
  public static final String HEADER_CLASS_NAME = "X-Class-Name";

  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeHandler.class);

  private final DatasetTypeManager manager;
  private final LocationFactory locationFactory;
  private final CConfiguration cConf;
  private final String archiveDirBase;

  @Inject
  public DatasetTypeHandler(DatasetTypeManager manager, LocationFactory locationFactory, CConfiguration conf) {
    this.manager = manager;
    this.locationFactory = locationFactory;
    this.cConf = conf;
    String dataFabricDir = conf.get(Constants.Dataset.Manager.OUTPUT_DIR);
    this.archiveDirBase = dataFabricDir + "/archive";
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
  public void listModules(HttpRequest request, final HttpResponder responder) {
    // Sorting by name for convenience
    List<DatasetModuleMeta> list = Lists.newArrayList(manager.getModules());
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
  public void deleteModules(HttpRequest request, final HttpResponder responder) {
    try {
      manager.deleteModules();
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (DatasetModuleConflictException e) {
      responder.sendError(HttpResponseStatus.CONFLICT, e.getMessage());
    }
  }

  @PUT
  @Path("/data/modules/{name}")
  public BodyConsumer addModule(final HttpRequest request, final HttpResponder responder,
                                @PathParam("name") final String name) throws IOException {

    final String className = request.getHeader(HEADER_CLASS_NAME);
    if (className == null) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Required header 'class-name' is absent.",
                           ImmutableMultimap.of(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE));
      return null;
    }

    LOG.info("Adding module {}, class name: {}", name, className);

    DatasetModuleMeta existing = manager.getModule(name);
    if (existing != null) {
      String message = String.format("Cannot add module %s: module with same name already exists: %s",
                                     name, existing);
      LOG.warn(message);
      responder.sendString(HttpResponseStatus.CONFLICT, message,
                           ImmutableMultimap.of(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE));
      return null;
    }

    // Store uploaded content to a local temp file
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException("Could not create temporary directory at: " + tempDir);
    }

    return new AbstractBodyConsumer(File.createTempFile("dataset-", ".jar", tempDir)) {
      @Override
      protected void onFinish(HttpResponder responder, File uploadedFile) throws Exception {
        Location archiveDir = locationFactory.create(archiveDirBase).append("account_placeholder");
        String archiveName = name + ".jar";
        Location archive = archiveDir.append(archiveName);

        // Copy uploaded content to a temporary location
        Location tmpLocation = archive.getTempFile(".tmp");
        try {
          Locations.mkdirsIfNotExists(archiveDir);

          LOG.debug("Copy from {} to {}", uploadedFile, tmpLocation.toURI());
          Files.copy(uploadedFile, Locations.newOutputSupplier(tmpLocation));

          // Check if the module exists one more time to minimize the window of possible conflict
          if (manager.getModule(name) == null) {
            // Finally, move archive to final location
            LOG.debug("Storing module {} jar at {}", name, archive.toURI());
            if (tmpLocation.renameTo(archive) == null) {
              throw new IOException(String.format("Could not move archive from location: %s, to location: %s",
                                                  tmpLocation.toURI(), archive.toURI()));
            }

            manager.addModule(name, className, archive);
            // todo: response with DatasetModuleMeta of just added module (and log this info)
            LOG.info("Added module {}", name);
            responder.sendStatus(HttpResponseStatus.OK);

          } else {
            throw new DatasetModuleConflictException(
              String.format("Cannot add module %s: module with same name already exists", name)
            );
          }

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
  public void deleteModule(HttpRequest request, final HttpResponder responder, @PathParam("name") String name) {
    boolean deleted;
    try {
      deleted = manager.deleteModule(name);
    } catch (DatasetModuleConflictException e) {
      responder.sendError(HttpResponseStatus.CONFLICT, e.getMessage());
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
  public void getModuleInfo(HttpRequest request, final HttpResponder responder, @PathParam("name") String name) {
    DatasetModuleMeta moduleMeta = manager.getModule(name);
    if (moduleMeta == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      responder.sendJson(HttpResponseStatus.OK, moduleMeta);
    }
  }

  @GET
  @Path("/data/types")
  public void listTypes(HttpRequest request, final HttpResponder responder) {
    // Sorting by name for convenience
    List<DatasetTypeMeta> list = Lists.newArrayList(manager.getTypes());
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
  public void getTypeInfo(HttpRequest request, final HttpResponder responder,
                      @PathParam("name") String name) {

    DatasetTypeMeta typeMeta = manager.getTypeInfo(name);
    if (typeMeta == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      responder.sendJson(HttpResponseStatus.OK, typeMeta);
    }
  }

}
