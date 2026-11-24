/*
 * Copyright © 2014-2026 Cask Data, Inc.
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

package io.cdap.cdap.common.http;

import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Security.Encryption;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.encryption.WorkerDecryptionScope;
import io.cdap.cdap.common.encryption.guice.LazyDelegateAeadCipher;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Credential.CredentialType;
import io.cdap.cdap.security.spi.authentication.AccessTokenIdentityValidator;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authentication.ValidatedTokenIdentity;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuditLogRequest;
import io.cdap.cdap.security.spi.encryption.CipherException;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.util.Queue;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An UpstreamHandler that verifies the userId and runtime credential in a request header and
 * updates the {@code SecurityRequestContext}.
 */
public class AuthenticationChannelHandler extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationChannelHandler.class);

  private static final String EMPTY_USER_ID = "CDAP-empty-user-id";
  private static final Credential EMPTY_USER_CREDENTIAL = new Credential(
      "CDAP-empty-user-credential",
      Credential.CredentialType.INTERNAL);
  private static final String EMPTY_USER_IP = "CDAP-empty-user-ip";
  static final String AUDIT_LOG_REQ_BUILDER_ATTR = "AUDIT_LOG_REQ_BUILDER";
  static final String AUDIT_LOG_USER_IP_ATTR = "AUDIT_LOG_USER_IP";
  static final String CDAP_USER_ID_ATTR = "CDAP_USER_ID";
  static final String CDAP_USER_CREDENTIAL_ATTR = "CDAP_USER_CREDENTIAL";
  static final String AUDIT_LOG_CONTEXT_QUEUE_ATTR = "AUDIT_LOG_CONTEXT_QUEUE";

  private final boolean internalAuthEnabled;
  private final boolean auditLoggingEnabled;
  private final boolean workerDecryptionEnabled;
  private final AuditLogWriter auditLogWriter;
  private final AeadCipher userEncryptionAeadCipher;
  @Nullable
  private final AccessTokenIdentityValidator tokenIdentityValidator;

  /**
   * Constructor for {@link AuthenticationChannelHandler}.
   */
  public AuthenticationChannelHandler(boolean internalAuthEnabled, boolean auditLoggingEnabled,
      boolean workerDecryptionEnabled, AuditLogWriter auditLogWriter,
      AeadCipher userEncryptionAeadCipher) {
    this(internalAuthEnabled, auditLoggingEnabled, workerDecryptionEnabled, auditLogWriter,
        userEncryptionAeadCipher, null);
  }

  /**
   * Constructor for {@link AuthenticationChannelHandler} with access token validation.
   *
   * @param internalAuthEnabled whether internal authentication enforcement is enabled
   * @param auditLoggingEnabled whether dataplane audit logging is enabled
   * @param workerDecryptionEnabled whether worker decryption and token verification are enabled
   * @param auditLogWriter writer for audit log events
   * @param userEncryptionAeadCipher cipher for decrypting user and worker credentials
   * @param tokenIdentityValidator validator for CDAP access tokens, if available
   */
  public AuthenticationChannelHandler(boolean internalAuthEnabled, boolean auditLoggingEnabled,
      boolean workerDecryptionEnabled, AuditLogWriter auditLogWriter,
      AeadCipher userEncryptionAeadCipher,
      @Nullable AccessTokenIdentityValidator tokenIdentityValidator) {
    this.internalAuthEnabled = internalAuthEnabled;
    this.auditLoggingEnabled = auditLoggingEnabled;
    this.workerDecryptionEnabled = workerDecryptionEnabled;
    this.auditLogWriter = auditLogWriter;
    this.userEncryptionAeadCipher = userEncryptionAeadCipher;
    this.tokenIdentityValidator = tokenIdentityValidator;
  }

  /**
   * Verifies the runtime credential header and populates {@link SecurityRequestContext}. Returns a
   * 401 response if the credential or identity is invalid.
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    SecurityRequestContext.reset();

    // TODO: CDAP-21121 ensure request is authorized before sending response
    if (msg instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) msg;
      ResolvedIdentity resolvedIdentity;
      try {
        resolvedIdentity = resolveIdentity(ctx, request);
      } catch (Throwable t) {
        clearIdentityAttributes(ctx);
        throw t;
      }

      SecurityRequestContext.setUserId(resolvedIdentity.userId);
      SecurityRequestContext.setUserCredential(resolvedIdentity.credential);
      SecurityRequestContext.setUserIp(resolvedIdentity.userIp);
      ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_USER_IP_ATTR))
          .set(resolvedIdentity.userIp);
      ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_ID_ATTR)).set(resolvedIdentity.userId);
      ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_CREDENTIAL_ATTR))
          .set(resolvedIdentity.credential);
    } else {
      Object userIpObj = ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_USER_IP_ATTR)).get();
      Object userIdObj = ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_ID_ATTR)).get();
      Object userCredentialObj =
          ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_CREDENTIAL_ATTR)).get();
      if (userIpObj != null) {
        SecurityRequestContext.setUserIp((String) userIpObj);
      }
      if (userIdObj != null && userCredentialObj != null) {
        SecurityRequestContext.setUserId((String) userIdObj);
        SecurityRequestContext.setUserCredential((Credential) userCredentialObj);
      }
    }

    try {
      ctx.fireChannelRead(msg);
    } finally {
      setAuditLogMetaDataInChannel(ctx);
      SecurityRequestContext.reset();
    }
  }

  private ResolvedIdentity resolveIdentity(ChannelHandlerContext ctx, HttpRequest request) {
    String headerUserId = request.headers().get(Constants.Security.Headers.USER_ID);
    String headerUserIp = request.headers().get(Constants.Security.Headers.USER_IP);
    String authHeader = request.headers().get(Constants.Security.Headers.RUNTIME_TOKEN);

    LOG.trace("Got user ID '{}' user IP '{}' from IP '{}' and authorization header length '{}'",
        headerUserId, headerUserIp, ctx.channel().remoteAddress(),
        authHeader == null ? "NULL" : authHeader.length());

    if (!internalAuthEnabled) {
      return resolveUnenforcedIdentity(request, headerUserId, headerUserIp, authHeader);
    }

    String currentUserIp = headerUserIp != null ? headerUserIp : EMPTY_USER_IP;
    if (authHeader == null) {
      if (headerUserId != null) {
        LOG.debug("Ignoring unverified {} header '{}' because {} is absent",
            Constants.Security.Headers.USER_ID, headerUserId,
            Constants.Security.Headers.RUNTIME_TOKEN);
      }
      return new ResolvedIdentity(EMPTY_USER_ID, EMPTY_USER_CREDENTIAL, currentUserIp);
    }

    String trimmedAuthHeader = authHeader.trim();
    int idx = trimmedAuthHeader.indexOf(' ');
    if (idx <= 0) {
      LOG.error("Invalid Authorization header format for {}@{}",
          headerUserId != null ? headerUserId : EMPTY_USER_ID, currentUserIp);
      throw new UnauthenticatedException("Invalid Authorization header format");
    }

    String credentialTypeStr = trimmedAuthHeader.substring(0, idx);
    CredentialType credentialType;
    try {
      credentialType = CredentialType.fromQualifiedName(credentialTypeStr);
    } catch (IllegalArgumentException e) {
      LOG.error("Invalid credential type in Authorization header: {}", credentialTypeStr);
      throw new UnauthenticatedException(e);
    }

    String credentialValue = trimmedAuthHeader.substring(idx + 1).trim();
    if (credentialValue.isEmpty()) {
      throw new UnauthenticatedException("Empty credential value in Authorization header");
    }

    // Worker pods (workerDecryptionEnabled == false) receive opaque worker-encrypted credentials
    // that can only be decrypted and verified when calling back to system services.
    if (!workerDecryptionEnabled) {
      String workerUserId = headerUserId != null ? headerUserId : EMPTY_USER_ID;
      return new ResolvedIdentity(workerUserId,
          new Credential(credentialValue, credentialType), currentUserIp);
    }

    Credential unwrappedCredential = validateAndGetCredential(request, headerUserId,
        credentialType, credentialValue);
    return verifySystemServiceCredential(headerUserId, currentUserIp, unwrappedCredential);
  }

  private ResolvedIdentity resolveUnenforcedIdentity(HttpRequest request,
      @Nullable String headerUserId, @Nullable String headerUserIp, @Nullable String authHeader) {
    Credential currentUserCredential = null;
    if (authHeader != null) {
      String trimmedAuthHeader = authHeader.trim();
      int idx = trimmedAuthHeader.indexOf(' ');
      if (idx <= 0) {
        LOG.error("Invalid Authorization header format for {}@{}", headerUserId, headerUserIp);
      } else {
        String credentialTypeStr = trimmedAuthHeader.substring(0, idx);
        try {
          CredentialType credentialType = CredentialType.fromQualifiedName(credentialTypeStr);
          String credentialValue = trimmedAuthHeader.substring(idx + 1).trim();
          if (!workerDecryptionEnabled) {
            currentUserCredential = new Credential(credentialValue, credentialType);
          } else {
            currentUserCredential = validateAndGetCredential(request, headerUserId, credentialType,
                credentialValue);
          }
        } catch (IllegalArgumentException e) {
          LOG.error("Invalid credential type in Authorization header: {}", credentialTypeStr);
          throw new UnauthenticatedException(e);
        }
      }
    }
    return new ResolvedIdentity(headerUserId, currentUserCredential, headerUserIp);
  }

  private ResolvedIdentity verifySystemServiceCredential(@Nullable String headerUserId,
      String currentUserIp, Credential credential) {
    switch (credential.getType()) {
      case INTERNAL:
        if (tokenIdentityValidator == null) {
          String resolvedUserId = headerUserId != null ? headerUserId : EMPTY_USER_ID;
          return new ResolvedIdentity(resolvedUserId, credential, currentUserIp);
        }
        ValidatedTokenIdentity internalIdentity =
            tokenIdentityValidator.validate(credential.getValue());
        if (!internalIdentity.isInternal()) {
          throw new UnauthenticatedException(
              "Expected internal access token for CDAP-Internal credential");
        }
        if (headerUserId != null && !headerUserId.equals(internalIdentity.getUsername())) {
          throw new UnauthenticatedException(String.format(
              "Header user ID '%s' does not match internal token identity '%s'",
              headerUserId, internalIdentity.getUsername()));
        }
        return new ResolvedIdentity(internalIdentity.getUsername(), credential, currentUserIp);

      case EXTERNAL_ENCRYPTED:
        if (headerUserId == null || headerUserId.isEmpty()) {
          throw new UnauthenticatedException(
              "Missing user ID header for encrypted external credential");
        }
        if (userEncryptionAeadCipher == null
            || userEncryptionAeadCipher instanceof NoOpAeadCipher) {
          throw new UnauthenticatedException(
              "Received encrypted user credential when user credential encryption is disabled");
        }
        try {
          userEncryptionAeadCipher.decryptStringFromBase64(credential.getValue(),
              Encryption.userCredentialAssociatedData(headerUserId));
        } catch (CipherException | IllegalArgumentException e) {
          throw new UnauthenticatedException(
              "Failed to verify encrypted user credential for user " + headerUserId, e);
        }
        return new ResolvedIdentity(headerUserId, credential, currentUserIp);

      case EXTERNAL:
        if (tokenIdentityValidator == null) {
          throw new UnauthenticatedException(
              "Unencrypted external credentials cannot be verified without a token validator");
        }
        ValidatedTokenIdentity externalIdentity =
            tokenIdentityValidator.validate(credential.getValue());
        if (headerUserId != null && !headerUserId.equals(externalIdentity.getUsername())) {
          throw new UnauthenticatedException(String.format(
              "Header user ID '%s' does not match external token identity '%s'",
              headerUserId, externalIdentity.getUsername()));
        }
        return new ResolvedIdentity(externalIdentity.getUsername(), credential, currentUserIp);

      default:
        throw new UnauthenticatedException(
            "Unsupported runtime credential type: " + credential.getType());
    }
  }

  /**
   * If Audit logging is enabled then it sends the collection of audit events stored in {@link
   * SecurityRequestContext} or Attribute of channel to get stored in a messaging system.
   */
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    publishAuditLogRequest(ctx);
    super.write(ctx, msg, promise);
  }

  /**
   * Need to handle for the case when "write" is not called in the netty channel.
   */
  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    publishAuditLogRequest(ctx);
    super.close(ctx, promise);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.error("Got exception: {}", cause.getMessage(), cause);
    clearIdentityAttributes(ctx);
    // TODO: add WWW-Authenticate header for 401 response -  REACTOR-900
    HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
        HttpResponseStatus.UNAUTHORIZED);
    HttpUtil.setContentLength(response, 0);
    HttpUtil.setKeepAlive(response, false);
    ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Check the audit log attribute attached to a channel. If it's not null, publish it and set it to
   * null.
   */
  private void publishAuditLogRequest(ChannelHandlerContext ctx) throws IOException {
    if (!auditLoggingEnabled) {
      return;
    }
    AuditLogRequest auditLogRequest = getAuditLogRequest(ctx);
    if (auditLogRequest != null) {
      auditLogWriter.publish(auditLogRequest);
    }
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private AuditLogRequest getAuditLogRequest(ChannelHandlerContext ctx) {
    Object auditLogContextsQueueAttr =
        ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_CONTEXT_QUEUE_ATTR)).get();

    // If NO audit logs, then return NULL.
    if (auditLogContextsQueueAttr == null) {
      return null;
    }

    Queue<AuditLogContext> auditLogContextsQueue =
        (Queue<AuditLogContext>) auditLogContextsQueueAttr;

    Object userIpObj = ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_USER_IP_ATTR)).get();
    String userIp = userIpObj == null ? SecurityRequestContext.getUserIp() : (String) userIpObj;

    // Check Attr for AuditLogRequest Builder.
    Object builderObj = ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_REQ_BUILDER_ATTR)).get();
    AuditLogRequest.Builder builder =
        builderObj == null
            ? SecurityRequestContext.getAuditLogRequestBuilder()
            : (AuditLogRequest.Builder) builderObj;

    // If the AuditLogContextsQueue is NOT empty, then the AuditLogRequest.Builder should NEVER be
    // null. It should either come from ATTR of pipeline or SecurityRequestContext.
    if (builder == null) {
      LOG.error("The meta data required to publish audit logs are missing or null. Following "
              + "Audit logs will be skipped: {}",
          auditLogContextsQueue.stream().map(String::valueOf).collect(Collectors.joining(", ")));
      return null;
    }

    clearAttributes(ctx);

    return builder
        .userIp(userIp)
        .auditLogContextQueue(auditLogContextsQueue)
        .build();
  }

  /**
   * Stores metadata from SecurityRequestContext inside channelRead's finally block.
   */
  private void setAuditLogMetaDataInChannel(ChannelHandlerContext ctx) {
    if (!auditLoggingEnabled) {
      return;
    }

    Queue<AuditLogContext> auditLogContextQueue = SecurityRequestContext.getAuditLogQueue();

    // In either case, if Queue from SecurityRequestContext is empty, then no Audit Logs.
    if (!auditLogContextQueue.isEmpty()) {
      ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_CONTEXT_QUEUE_ATTR))
          .set(auditLogContextQueue);
    }

    // Store all audit metadata info stored in AuditLogRequest.Builder in ATTR from
    // AuditLogSetterHook#postCall so we don't lose metadata information if reset on
    // SecurityRequestContext or if thread changes in close/write.
    AuditLogRequest.Builder builder = SecurityRequestContext.getAuditLogRequestBuilder();
    if (builder != null) {
      ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_REQ_BUILDER_ATTR)).set(builder);
    }
  }

  private void clearIdentityAttributes(ChannelHandlerContext ctx) {
    ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_USER_IP_ATTR)).set(null);
    ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_ID_ATTR)).set(null);
    ctx.channel().attr(AttributeKey.valueOf(CDAP_USER_CREDENTIAL_ATTR)).set(null);
  }

  private void clearAttributes(ChannelHandlerContext ctx) {
    ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_REQ_BUILDER_ATTR)).set(null);
    ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_CONTEXT_QUEUE_ATTR)).set(null);
    clearIdentityAttributes(ctx);
  }

  /**
   * Tries to decrypt the credential value with worker associatedData. If decryption is successful,
   * returns the decrypted string, else returns original string.
   */
  private String decryptIfPossible(String credentialValue, byte[] associatedData) {
    if (userEncryptionAeadCipher instanceof LazyDelegateAeadCipher) {
      try {
        return userEncryptionAeadCipher.decryptStringFromBase64(credentialValue, associatedData);
      } catch (CipherException | IllegalArgumentException e) {
        return credentialValue;
      }
    }
    return credentialValue;
  }

  /**
   * Determines the worker type by attempting to decrypt the credential with worker Associated Data
   * bound to {@code userId}.
   */
  private ImmutablePair<WorkerDecryptionScope, String> determineWorkerTypeAndGetCredential(
      String credentialValue, @Nullable String userId) {

    // Try to decrypt with Task Worker associated data first.
    String decrypted = decryptIfPossible(credentialValue,
        Encryption.taskWorkerAssociatedData(userId));
    if (!decrypted.equals(credentialValue)) {
      return new ImmutablePair<>(WorkerDecryptionScope.TASK_WORKER, decrypted);
    }

    // If that fails, try to decrypt with Preview runner associated data.
    decrypted = decryptIfPossible(credentialValue,
        Encryption.previewRunnerAssociatedData(userId));
    if (!decrypted.equals(credentialValue)) {
      return new ImmutablePair<>(WorkerDecryptionScope.PREVIEW_RUNNER, decrypted);
    }

    // If both fail, it's not a worker call.
    return new ImmutablePair<>(WorkerDecryptionScope.NONE, credentialValue);
  }

  private Credential validateAndGetCredential(HttpRequest request, @Nullable String userId,
      CredentialType credentialType, String credentialValue) {
    String expectedDecryptionHeader = request.headers().get(HttpHeaderNames.WORKER_DECRYPTION_HDR);
    WorkerDecryptionScope expectedWorkerDecryptionScope;
    try {
      expectedWorkerDecryptionScope = (expectedDecryptionHeader == null)
          ? WorkerDecryptionScope.NONE
          : WorkerDecryptionScope.valueOf(expectedDecryptionHeader);
    } catch (IllegalArgumentException e) {
      throw new UnauthenticatedException("Invalid worker decryption scope header", e);
    }

    // 1. Determine the actual worker type by trying to decrypt the credential.
    ImmutablePair<WorkerDecryptionScope, String> workerInfo = determineWorkerTypeAndGetCredential(
        credentialValue, userId);
    WorkerDecryptionScope actualWorkerType = workerInfo.getFirst();
    String decryptedCredentialValue = workerInfo.getSecond();

    // 2. If it's a worker, verify the target URI exemption scope allows this worker type.
    if (actualWorkerType != WorkerDecryptionScope.NONE) {
      boolean isAuthorized = (expectedWorkerDecryptionScope == WorkerDecryptionScope.ANY_WORKER)
          || (expectedWorkerDecryptionScope == actualWorkerType);

      if (!isAuthorized) {
        throw new UnauthenticatedException(String.format(
            "Request from a %s was denied because the URI '%s' is not "
                + "exempt for this worker type. Expected %s", actualWorkerType,
            request.uri(), expectedWorkerDecryptionScope));
      }
    }
    return new Credential(decryptedCredentialValue, credentialType);
  }

  private static final class ResolvedIdentity {
    private final String userId;
    private final Credential credential;
    private final String userIp;

    private ResolvedIdentity(@Nullable String userId, @Nullable Credential credential,
        @Nullable String userIp) {
      this.userId = userId;
      this.credential = credential;
      this.userIp = userIp;
    }
  }
}
