/*
 * Copyright © 2020-2026 Cask Data, Inc.
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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.GoneException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The http handler for routing CDAP service requests from program runtime.
 */
@Path(Constants.Gateway.INTERNAL_API_VERSION_3
    + "/runtime/namespaces/{namespace}/apps/{app}/versions/{version}/{program-type}/{program}/runs/{run}")
public class RuntimeServiceRoutingHandler extends AbstractServiceRoutingHandler {

  static final int MAX_BUFFERED_BODY_BYTES = 10 * 1024 * 1024;

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeServiceRoutingHandler.class);

  private final RuntimeRequestValidator requestValidator;
  private final InternalAuthenticator internalAuthenticator;
  private final RuntimeCallbackAllowlist allowlist;
  private final RuntimeValidationReporter validationReporter;
  private final int maxBufferedBodyBytes;

  @Inject
  RuntimeServiceRoutingHandler(DiscoveryServiceClient discoveryServiceClient,
      RuntimeRequestValidator requestValidator,
      InternalAuthenticator internalAuthenticator,
      RuntimeCallbackAllowlist allowlist,
      RuntimeValidationReporter validationReporter) {
    this(discoveryServiceClient, requestValidator, internalAuthenticator,
        allowlist, validationReporter, MAX_BUFFERED_BODY_BYTES);
  }

  RuntimeServiceRoutingHandler(DiscoveryServiceClient discoveryServiceClient,
      RuntimeRequestValidator requestValidator,
      InternalAuthenticator internalAuthenticator,
      RuntimeCallbackAllowlist allowlist,
      RuntimeValidationReporter validationReporter,
      int maxBufferedBodyBytes) {
    super(discoveryServiceClient);
    Preconditions.checkArgument(maxBufferedBodyBytes > 0,
        "maxBufferedBodyBytes must be positive: %s", maxBufferedBodyBytes);
    this.requestValidator = requestValidator;
    this.internalAuthenticator = internalAuthenticator;
    this.allowlist = allowlist;
    this.validationReporter = validationReporter;
    this.maxBufferedBodyBytes = maxBufferedBodyBytes;
  }

  /**
   * Handles GET and DELETE calls from program runtime to access CDAP services.
   * It verifies the request and forwards the call to the internal CDAP service.
   */
  @Path("/services/{service}/**")
  @GET
  @DELETE
  public void routeService(HttpRequest request, HttpResponder responder,
      @PathParam("namespace") String namespace, @PathParam("app") String app,
      @PathParam("version") String version,
      @PathParam("program-type") String programType,
      @PathParam("program") String program, @PathParam("run") String run,
      @PathParam("service") String service) throws Exception {
    PreparedRoute prepared = prepareRoute(request, namespace, app, version,
        programType, program, run, service, false);
    routeService(prepared.forwardRequest, responder, service, prepared.servicePath);
  }

  /**
   * Handles PUT and POST calls from program runtime to access CDAP services.
   * It verifies the request and forwards the call to internal CDAP services.
   */
  @Path("/services/{service}/**")
  @PUT
  @POST
  public BodyConsumer routeServiceWithBody(HttpRequest request,
      HttpResponder responder, @PathParam("namespace") String namespace,
      @PathParam("app") String app, @PathParam("version") String version,
      @PathParam("program-type") String programType,
      @PathParam("program") String program, @PathParam("run") String run,
      @PathParam("service") String service) throws Exception {
    PreparedRoute prepared = prepareRoute(request, namespace, app, version,
        programType, program, run, service, true);
    if (prepared.match != null && prepared.match.requiresBodyCheck()) {
      return new BufferingBodyConsumer(prepared.forwardRequest, service, prepared.servicePath,
          prepared.tokenRunId, prepared.match);
    }
    return routeServiceWithBody(prepared.forwardRequest, responder, service, prepared.servicePath);
  }

  private PreparedRoute prepareRoute(HttpRequest request, String namespace, String app,
      String version, String programType, String program, String run, String service,
      boolean hasBody) throws BadRequestException, AccessException, GoneException {
    ValidatedRuntimeRequest validated = validateRequest(request, namespace, app, version,
        programType, program, run);
    String prefix = getPrefix(namespace, app, version, programType, program, run, service);
    String servicePath = extractServicePath(request.uri(), prefix);
    if (!validated.isRuntimeToken()) {
      return new PreparedRoute(request, servicePath, null, null);
    }

    ProgramRunId tokenRunId = validated.getTokenRunId();
    RuntimeCallbackAllowlist.MatchResult match = allowlist.match(
        servicePath, service, request.method().name(), tokenRunId);
    if (!match.isAllowed() || (!hasBody && match.requiresBodyCheck())) {
      boolean enforce = validationReporter.reportFailure(
          RuntimeValidationFailure.API_NOT_ALLOWED,
          tokenRunId, service, request.method().name(), match.getLogPath());
      if (enforce) {
        throw new UnauthorizedException("Callback API is not allowed");
      }
    }
    HttpRequest forwardRequest = buildExchangedRequest(request);
    return new PreparedRoute(forwardRequest, servicePath, tokenRunId, match);
  }

  /**
   * Validates the request.
   *
   * @throws GoneException if the run already finished
   */
  private ValidatedRuntimeRequest validateRequest(HttpRequest request, String namespace,
      String app, String version, String programType, String program,
      String run) throws BadRequestException, AccessException, GoneException {
    ApplicationId appId = new NamespaceId(namespace).app(app, version);
    ProgramRunId programRunId = new ProgramRunId(appId,
        ProgramType.valueOfCategoryName(programType, BadRequestException::new),
        program, run);
    return requestValidator.validate(programRunId, request);
  }

  /**
   * Builds a fresh downstream request for a {@code RUNTIME}-token caller containing only
   * {@code Content-Type}, {@code If-Modified-Since}, system-minted internal authentication headers,
   * and {@code CDAP-UserId: runtime}.
   */
  private HttpRequest buildExchangedRequest(HttpRequest request) {
    HttpHeaders headers = new DefaultHttpHeaders();
    String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
    if (contentType != null) {
      headers.set(HttpHeaderNames.CONTENT_TYPE, contentType);
    }
    String ifModifiedSince = request.headers().get(HttpHeaderNames.IF_MODIFIED_SINCE);
    if (ifModifiedSince != null) {
      headers.set(HttpHeaderNames.IF_MODIFIED_SINCE, ifModifiedSince);
    }
    AuthorizationUtil.callAsSystem(() -> {
      internalAuthenticator.applyInternalAuthenticationHeaders(headers::set);
      return null;
    });
    // Overwrite the system USER_ID set by applyInternalAuthenticationHeaders so downstream
    // services authorize the forwarded call as the runtime identity.
    headers.set(Constants.Security.Headers.USER_ID,
        Constants.Security.Authentication.RUNTIME_IDENTITY);
    return new DefaultHttpRequest(request.protocolVersion(), request.method(), request.uri(),
        headers);
  }

  private static String getPrefix(String namespace, String app, String version,
      String programType, String program, String run, String service) {
    return String.format(
        "%s/runtime/namespaces/%s/apps/%s/versions/%s/%s/%s/runs/%s/services/%s",
        Constants.Gateway.INTERNAL_API_VERSION_3, namespace, app, version,
        programType, program, run, service);
  }

  private static String extractServicePath(String rawUri, String prefix)
      throws BadRequestException {
    if (rawUri == null || !rawUri.startsWith(prefix + "/")) {
      throw new BadRequestException("Invalid service routing path");
    }
    return rawUri.substring(prefix.length());
  }

  private static final class PreparedRoute {

    private final HttpRequest forwardRequest;
    private final String servicePath;
    @Nullable
    private final ProgramRunId tokenRunId;
    @Nullable
    private final RuntimeCallbackAllowlist.MatchResult match;

    private PreparedRoute(HttpRequest forwardRequest, String servicePath,
        @Nullable ProgramRunId tokenRunId,
        @Nullable RuntimeCallbackAllowlist.MatchResult match) {
      this.forwardRequest = forwardRequest;
      this.servicePath = servicePath;
      this.tokenRunId = tokenRunId;
      this.match = match;
    }
  }

  /**
   * Buffers a bounded request body so allowlist body rules can be verified before opening the
   * downstream service call and replaying the buffered bytes. In report-only mode, if a body
   * exceeds {@code maxBufferedBodyBytes}, switches to streaming pass-through so memory stays
   * bounded while the full request is still forwarded.
   */
  private final class BufferingBodyConsumer extends BodyConsumer {

    private final HttpRequest forwardRequest;
    private final String service;
    private final String servicePath;
    private final ProgramRunId tokenRunId;
    private final RuntimeCallbackAllowlist.MatchResult match;
    private final CompositeByteBuf buffer;
    @Nullable
    private BodyConsumer streamingDownstream;
    @Nullable
    private HttpResponseStatus errorStatus;
    @Nullable
    private String errorMessage;
    private boolean released;

    private BufferingBodyConsumer(HttpRequest forwardRequest, String service, String servicePath,
        ProgramRunId tokenRunId, RuntimeCallbackAllowlist.MatchResult match) {
      this.forwardRequest = forwardRequest;
      this.service = service;
      this.servicePath = servicePath;
      this.tokenRunId = tokenRunId;
      this.match = match;
      this.buffer = Unpooled.compositeBuffer();
    }

    @Override
    public void chunk(ByteBuf request, HttpResponder responder) {
      if (errorStatus != null) {
        return;
      }
      int readable = request.readableBytes();
      if (readable == 0) {
        return;
      }
      if (streamingDownstream != null) {
        if (!relayChunk(streamingDownstream, request, responder)) {
          streamingDownstream = null;
        }
        return;
      }
      if (released) {
        return;
      }
      if (readable > maxBufferedBodyBytes - buffer.readableBytes()) {
        boolean enforce = validationReporter.reportFailure(
            RuntimeValidationFailure.API_NOT_ALLOWED,
            tokenRunId, service, forwardRequest.method().name(), match.getLogPath());
        if (enforce) {
          releaseBuffer();
          recordError(HttpResponseStatus.FORBIDDEN,
              "Callback API request body exceeds maximum allowed size of "
                  + maxBufferedBodyBytes + " bytes");
          return;
        }
        switchToStreamingDownstream(request, responder);
        return;
      }
      buffer.addComponent(true, request.retain());
    }

    @Override
    public void finished(HttpResponder responder) {
      try {
        if (errorStatus == null) {
          if (streamingDownstream != null) {
            BodyConsumer downstream = streamingDownstream;
            streamingDownstream = null;
            finishDownstream(downstream, responder);
          } else {
            finishBuffered(responder);
          }
        }
        // Re-check errorStatus because finishDownstream or finishBuffered above may record an error.
        if (errorStatus != null) {
          responder.sendString(errorStatus, Strings.nullToEmpty(errorMessage));
        }
      } finally {
        releaseBuffer();
      }
    }

    @Override
    public void handleError(Throwable cause) {
      releaseBuffer();
      if (streamingDownstream != null && errorStatus == null) {
        BodyConsumer downstream = streamingDownstream;
        streamingDownstream = null;
        downstream.handleError(cause);
      }
      LOG.warn("Exception raised while buffering callback request for service '{}'",
          service, cause);
    }

    private void finishBuffered(HttpResponder responder) {
      byte[] bodyBytes = new byte[buffer.readableBytes()];
      buffer.getBytes(buffer.readerIndex(), bodyBytes);
      if (!allowlist.checkBody(match, tokenRunId, bodyBytes)) {
        boolean enforce = validationReporter.reportFailure(
            RuntimeValidationFailure.API_NOT_ALLOWED,
            tokenRunId, service, forwardRequest.method().name(), match.getLogPath());
        if (enforce) {
          recordError(HttpResponseStatus.FORBIDDEN, "Callback API request body is not allowed");
          return;
        }
      }
      BodyConsumer downstream = openDownstream(responder);
      if (downstream == null) {
        return;
      }
      if (buffer.isReadable() && !relayChunk(downstream, buffer, responder)) {
        return;
      }
      finishDownstream(downstream, responder);
    }

    private void switchToStreamingDownstream(ByteBuf currentChunk, HttpResponder responder) {
      BodyConsumer downstream;
      try {
        downstream = openDownstream(responder);
        if (downstream == null
            || (buffer.isReadable() && !relayChunk(downstream, buffer, responder))) {
          return;
        }
      } finally {
        releaseBuffer();
      }
      if (relayChunk(downstream, currentChunk, responder)) {
        streamingDownstream = downstream;
      }
    }

    @Nullable
    private BodyConsumer openDownstream(HttpResponder responder) {
      try {
        return RuntimeServiceRoutingHandler.this.routeServiceWithBody(
            forwardRequest, responder, service, servicePath);
      } catch (Exception e) {
        recordDownstreamError(null, e, "Failed to open downstream request to service '{}'");
        return null;
      }
    }

    private boolean relayChunk(BodyConsumer downstream, ByteBuf chunk, HttpResponder responder) {
      try {
        downstream.chunk(chunk, responder);
        return true;
      } catch (RuntimeException e) {
        recordDownstreamError(downstream, e, "Failed to relay callback request to service '{}'");
        return false;
      }
    }

    private void finishDownstream(BodyConsumer downstream, HttpResponder responder) {
      try {
        downstream.finished(responder);
      } catch (RuntimeException e) {
        recordDownstreamError(downstream, e, "Failed to finish relayed request to service '{}'");
      }
    }

    private void recordDownstreamError(@Nullable BodyConsumer downstream, Exception e,
        String warnFormat) {
      if (e instanceof HttpErrorStatusProvider) {
        recordError(
            HttpResponseStatus.valueOf(((HttpErrorStatusProvider) e).getStatusCode()),
            e.getMessage());
      } else {
        LOG.warn(warnFormat, service, e);
        recordError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            "Failed to forward callback request");
      }
      if (downstream != null) {
        downstream.handleError(e);
      }
    }

    private void recordError(HttpResponseStatus status, @Nullable String message) {
      errorStatus = status;
      errorMessage = message;
    }

    private void releaseBuffer() {
      if (!released) {
        released = true;
        buffer.release();
      }
    }
  }
}
