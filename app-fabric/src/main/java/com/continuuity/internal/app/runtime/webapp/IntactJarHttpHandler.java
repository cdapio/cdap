package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.archive.JarResources;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;

/**
 * Http service handler that serves files in deployed jar without exploding the jar.
 */
public class IntactJarHttpHandler extends AbstractHttpHandler implements JarHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IntactJarHttpHandler.class);

  private static final MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();

  private final Location jarLocation;
  private JarResources jarResources;
  private ServePathGenerator servePathGenerator;

  @Inject
  public IntactJarHttpHandler(@Assisted Location jarLocation) {
    this.jarLocation = jarLocation;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    try {
      jarResources = new JarResources(jarLocation);

      Predicate<String> fileExists = new Predicate<String>() {
        @Override
        public boolean apply(String file) {
          return file != null && jarResources.contains(file);
        }
      };

      servePathGenerator = new ServePathGenerator(Constants.Webapp.WEBAPP_DIR, fileExists);
    } catch (IOException e) {
      LOG.error("Got exception: ", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String getServePath(String hostHeader, String uri) {
    return servePathGenerator.getServePath(hostHeader, uri);
  }

  @GET
  @Path("**")
  public void serve(HttpRequest request, HttpResponder responder) {
    try {
      String path = request.getUri();
      if (path == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      if (path.startsWith("/") && path.length() > 1) {
        path = path.substring(1);
      }

      byte [] bytes = jarResources.getResource(path);

      if (bytes == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      responder.sendByteArray(HttpResponseStatus.OK, bytes,
                              ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE,
                                                   mimeTypesMap.getContentType(path)));

    } catch (Throwable t) {
      LOG.error("Got exception: ", t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
