package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.archive.JarResources;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URLConnection;

/**
 * Http service handler that serves files in deployed jar without exploding the jar.
 */
public class IntactJarHttpHandler extends AbstractHttpHandler implements WebappHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IntactJarHttpHandler.class);

  private Location jarLocation;
  private JarResources jarResources;

  @Override
  public void setJarLocation(Location jarLocation) {
    this.jarLocation = jarLocation;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    try {
      jarResources = new JarResources(jarLocation);
    } catch (IOException e) {
      LOG.error("Got exception: ", e);
      throw Throwables.propagate(e);
    }
  }

  @GET
  @Path("/.*")
  public void serve(HttpRequest request, HttpResponder responder) {
    try {

      if (request.getUri().equals("/status")) {
        responder.sendString(HttpResponseStatus.OK, "OK\n");
        return;
      }

      String file = new File(Constants.Webapp.WEBAPP_DIR, request.getUri()).getAbsolutePath();
      byte [] bytes = jarResources.getResource(file);

      String contentType = URLConnection.guessContentTypeFromStream(new ByteArrayInputStream(bytes));
      responder.sendByteArray(HttpResponseStatus.OK, bytes,
                              ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, contentType));

    } catch (Throwable t) {
      LOG.error("Got exception: ", t);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }
}
