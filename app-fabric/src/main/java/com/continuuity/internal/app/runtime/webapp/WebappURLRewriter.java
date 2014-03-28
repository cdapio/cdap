package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.http.HttpResponder;
import com.continuuity.http.URLRewriter;
import com.google.common.collect.ImmutableMultimap;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Rewrites incoming webapp URLs as Gateway URLs, if it is a Gateway call.
 * Otherwise, it resolves the jar path for the requested file.
 */
public class WebappURLRewriter implements URLRewriter {
  private final JarHttpHandler jarHttpHandler;

  public WebappURLRewriter(JarHttpHandler jarHttpHandler) {
    this.jarHttpHandler = jarHttpHandler;
  }

  @Override
  public boolean rewrite(HttpRequest request, HttpResponder responder) {
    String hostHeader = HttpHeaders.getHost(request);
    if (hostHeader == null) {
      return true;
    }

    String originalUri = request.getUri();
    String uri = jarHttpHandler.getServePath(hostHeader, originalUri);
    if (uri != null) {
      // Redirect requests that map to index.html without a trailing slash to url/
      if (!originalUri.endsWith("/") && !originalUri.endsWith("index.html") && uri.endsWith("index.html")) {
        responder.sendStatus(HttpResponseStatus.MOVED_PERMANENTLY,
                             ImmutableMultimap.of("Location", originalUri + "/"));
        return false;
      }
      request.setUri(uri);
    }

    return true;
  }
}
