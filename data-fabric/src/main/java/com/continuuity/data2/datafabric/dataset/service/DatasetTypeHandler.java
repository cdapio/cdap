package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.datafabric.dataset.type.DatasetModuleConflictException;
import com.continuuity.data2.datafabric.dataset.type.DatasetModuleMeta;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeMeta;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
  public static final String HEADER_CLASS_NAME = "X-Continuuity-Class-Name";

  private static final Logger LOG = LoggerFactory.getLogger(DatasetTypeHandler.class);

  private final DatasetTypeManager manager;
  private final LocationFactory locationFactory;
  private final String archiveDir;

  @Inject
  public DatasetTypeHandler(DatasetTypeManager manager,
                            LocationFactory locationFactory,
                            CConfiguration conf) {
    this.manager = manager;
    this.locationFactory = locationFactory;
    String dataFabricDir = conf.get(Constants.Dataset.Manager.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    this.archiveDir = dataFabricDir + "/archive";
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
  public void addModule(HttpRequest request, final HttpResponder responder,
                       @PathParam("name") String name) throws IOException {

    String className = request.getHeader(HEADER_CLASS_NAME);
    Preconditions.checkArgument(className != null, "Required header 'class-name' is absent.");
    LOG.info("Adding module {}, class name: {}", name, className);

    DatasetModuleMeta existing = manager.getModule(name);
    if (existing != null) {
      String message = String.format("Cannot add module %s: module with same name already exists: %s",
                                     name, existing);
      LOG.warn(message);
      responder.sendError(HttpResponseStatus.CONFLICT, message);
      return;
    }

    ChannelBuffer content = request.getContent();
    if (content == null) {
      LOG.warn("Cannot add module {}: content is null", name);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Content is null");
      return;
    }

    Location uploadDir = locationFactory.create(archiveDir).append("account_placeholder");
    String archiveName = name + ".jar";
    Location archive = uploadDir.append(archiveName);
    LOG.info("Storing module {} jar at {}", name, archive.toURI().toString());

    if (!uploadDir.exists() && !uploadDir.mkdirs()) {
      LOG.warn("Unable to create directory '{}'", uploadDir.getName());
    }

    InputStream inputStream = new ChannelBufferInputStream(content);
    try {
      // todo: store to temp file first and do some verifications? Or even datasetFramework should persist file?
      OutputStream outStream = archive.getOutputStream();
      try {
        ByteStreams.copy(inputStream, outStream);
      } finally {
        outStream.close();
      }
    } finally {
      inputStream.close();
    }

    try {
      manager.addModule(name, className, archive);
    } catch (DatasetModuleConflictException e) {
      responder.sendError(HttpResponseStatus.CONFLICT, e.getMessage());
      return;
    }
    // todo: response with DatasetModuleMeta of just added module (and log this info)
    LOG.info("Added module {}", name);
    responder.sendStatus(HttpResponseStatus.OK);
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
