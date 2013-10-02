package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.google.common.collect.ImmutableMultimap;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.File;
import java.net.URLConnection;

/**
 * Http service handler that serves files in deployed jar.
 */
public class WebappHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(WebappHandler.class);

  private final File serveDir;

  public WebappHandler(File serveDir) {
    this.serveDir = serveDir;
  }

  @GET
  @Path("/.*")
  public void serve(HttpRequest request, HttpResponder responder) {
    try {
      File file = new File(serveDir, request.getUri());

      if (request.getUri().equals("/")) {
        file = new File(serveDir, "index.html");
      }

      if (!file.exists()) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      if (!file.isFile()) {
        responder.sendStatus(HttpResponseStatus.FORBIDDEN);
        return;
      }

      responder.sendFile(file, ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE,
                                                    URLConnection.guessContentTypeFromName(file.getAbsolutePath())));

    } catch (Throwable t) {
      LOG.error("Got exception: ", t);
      responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage());
    }
  }

  @GET
  @Path("/status")
  public void status(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "OK\n");
  }
}
