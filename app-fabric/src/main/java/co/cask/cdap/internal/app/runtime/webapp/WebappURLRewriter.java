/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.webapp;

import co.cask.http.HttpResponder;
import co.cask.http.URLRewriter;
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
