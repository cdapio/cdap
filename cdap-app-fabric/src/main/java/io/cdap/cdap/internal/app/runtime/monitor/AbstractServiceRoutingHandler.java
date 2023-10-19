/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.Closeables;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.BodyConsumer;
import io.cdap.http.BodyProducer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URL;
import java.net.UnknownServiceException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AbstractServiceRoutingHandler} forwards  requests to CDAP services and
 * relays the response. It doesn't carry out any auth checks. Auth checks can be
 * performed by child classes or by the downstream CDAP service.
 */
public abstract class AbstractServiceRoutingHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractServiceRoutingHandler.class);

  private final LoadingCache<String, EndpointStrategy> endpointStrategyLoadingCache;

  /**
   * Handles requests for CDAP services by forwarding them to the correct
   * destinations and relaying the response.
   *
   * @param discoveryServiceClient Discovery client for getting CDAP service
   *                               addresses.
   */
  protected AbstractServiceRoutingHandler(
      DiscoveryServiceClient discoveryServiceClient) {
    this.endpointStrategyLoadingCache = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS)
        .build(new CacheLoader<String, EndpointStrategy>() {
          @Override
          public EndpointStrategy load(String key) {
            return new RandomEndpointStrategy(
                () -> discoveryServiceClient.discover(key));
          }
        });
  }

  /**
   * Handles calls without request body to access CDAP services. It forwards the
   * call to internal CDAP service.
   *
   * @param request the incoming request.
   * @param service the CDAP service to which the call is to be forwarded.
   * @param path    the url path for the service to forward the call to.
   */
  public void routeService(HttpRequest request, HttpResponder responder,
      String service, String path) throws Exception {
    LOG.trace("Got request to route to service '{}'", service);
    HttpURLConnection urlConn = openConnection(request, service, path);
    ResponseInfo responseInfo = new ResponseInfo(service, urlConn);
    responder.sendContent(
        HttpResponseStatus.valueOf(responseInfo.getResponseCode()),
        new RelayBodyProducer(urlConn.getURL(), responseInfo),
        responseInfo.getHeaders());
  }

  /**
   * Handles http calls with a request body to access CDAP services. It forwards
   * the call to internal CDAP services.
   *
   * @param request the incoming request.
   * @param service the CDAP service to which the call is to be forwarded.
   * @param path    the url path for the service to forward the call to.
   */
  public BodyConsumer routeServiceWithBody(HttpRequest request,
      HttpResponder responder, String service, String path) throws Exception {
    LOG.trace("Got request to route to service '{}'", service);
    HttpURLConnection urlConn = openConnection(request, service, path);
    urlConn.setDoOutput(true);
    OutputStream output;
    try {
      output = urlConn.getOutputStream();
    } catch (UnknownServiceException e) {
      throw new BadRequestException(e.getMessage(), e);
    } catch (IOException e) {
      // If fails to get output stream, treat it as service unavailable so that the client can retry
      throw new ServiceUnavailableException(service, e);
    }

    return new BodyConsumer() {

      @Override
      public void chunk(ByteBuf byteBuf, HttpResponder httpResponder) {
        try {
          byteBuf.readBytes(output, byteBuf.readableBytes());
        } catch (IOException e) {
          throw new ServiceUnavailableException(service, e);
        }
      }

      @Override
      public void finished(HttpResponder httpResponder) {
        try {
          output.close();
        } catch (IOException e) {
          throw new ServiceUnavailableException(service, e);
        }
        LOG.trace("Finished sending data to {}", urlConn.getURL());
        try {
          ResponseInfo responseInfo = new ResponseInfo(service, urlConn);
          responder.sendContent(
              HttpResponseStatus.valueOf(responseInfo.getResponseCode()),
              new RelayBodyProducer(urlConn.getURL(), responseInfo),
              responseInfo.getHeaders());
        } catch (BadRequestException e) {
          throw new ServiceException(e, HttpResponseStatus.BAD_REQUEST);
        }
      }

      @Override
      public void handleError(Throwable throwable) {
        LOG.warn("Exception raised for call to {}", urlConn.getURL(),
            throwable);
      }
    };
  }

  /**
   * Opens a {@link HttpURLConnection} to the given service for the given
   * program run.
   *
   * @throws BadRequestException if the request for service routing is not
   *                             valid
   */
  private HttpURLConnection openConnection(HttpRequest request, String service,
      String path) throws BadRequestException {
    Discoverable discoverable = endpointStrategyLoadingCache.getUnchecked(
        service).pick(2, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new ServiceUnavailableException(service);
    }

    URI uri = URIScheme.createURI(discoverable, path);
    LOG.trace("Routing request for service '{}' to uri '{}'.", service, uri);
    try {
      URL url = uri.toURL();
      HttpURLConnection urlConn;
      try {
        urlConn = (HttpURLConnection) url.openConnection();
      } catch (IOException e) {
        // If fail to open the connection, treat it as service unavailable so that the client can retry
        throw new ServiceUnavailableException(service);
      }

      if (urlConn instanceof HttpsURLConnection) {
        new HttpsEnabler().setTrustAll(true)
            .enable((HttpsURLConnection) urlConn);
      }
      for (Map.Entry<String, String> header : request.headers().entries()) {
        urlConn.setRequestProperty(header.getKey(), header.getValue());
      }
      urlConn.setRequestMethod(request.method().name());
      urlConn.setDoInput(true);

      return urlConn;

    } catch (MalformedURLException | ProtocolException e) {
      // This can only happen if the incoming request is bad
      throw new BadRequestException("Invalid request due to " + e.getMessage(),
          e);
    }
  }

  /**
   * A holder object for holding information related to the response from remote
   * service to which the request is routed.
   */
  private static final class ResponseInfo implements Closeable {

    private final HttpURLConnection urlConn;
    private final int responseCode;
    private final HttpHeaders headers;
    private final InputStream input;

    ResponseInfo(String serviceName, HttpURLConnection urlConn)
        throws BadRequestException, ServiceUnavailableException {
      InputStream is = null;
      try {
        this.responseCode = urlConn.getResponseCode();
      } catch (IOException e) {
        throw new ServiceUnavailableException(serviceName, e);
      }
      try {
        is = urlConn.getInputStream();
        if (this.responseCode >= 400) {
          Closeables.closeQuietly(is);
          is = null;
        }
      } catch (UnknownServiceException e) {
        throw new BadRequestException(e.getMessage(), e);
      } catch (IOException e) {
        // Intentionally empty catch. This happens when server return 404.
        // We handle all errors uniformly below.
      }
      this.input = is == null ? urlConn.getErrorStream() : is;

      // Copy all headers
      DefaultHttpHeaders headers = new DefaultHttpHeaders();
      for (Map.Entry<String, List<String>> entry : urlConn.getHeaderFields()
          .entrySet()) {
        if (entry.getKey() != null && entry.getValue() != null) {
          headers.add(entry.getKey(), entry.getValue());
        }
      }

      this.headers = headers;
      this.urlConn = urlConn;
    }

    int getResponseCode() {
      return responseCode;
    }

    HttpHeaders getHeaders() {
      return headers;
    }

    @Nullable
    InputStream getInput() {
      return input;
    }

    @Override
    public void close() {
      Closeables.closeQuietly(input);
      urlConn.disconnect();
    }
  }

  /**
   * A {@link BodyProducer} to relay response from a http call.
   */
  private static final class RelayBodyProducer extends BodyProducer {

    private final URL url;
    private final ResponseInfo responseInfo;

    private RelayBodyProducer(URL url, ResponseInfo responseInfo) {
      this.url = url;
      this.responseInfo = responseInfo;
    }

    @Override
    public ByteBuf nextChunk() throws Exception {
      if (responseInfo.getInput() == null) {
        return Unpooled.EMPTY_BUFFER;
      }
      ByteBuf buffer = Unpooled.buffer(8192);
      buffer.writeBytes(responseInfo.getInput(), buffer.writableBytes());
      return buffer;
    }

    @Override
    public void finished() {
      Closeables.closeQuietly(responseInfo);
    }

    @Override
    public void handleError(@Nullable Throwable cause) {
      LOG.warn("Exception raised when handling request to {}", url, cause);
      Closeables.closeQuietly(responseInfo);
    }
  }
}
