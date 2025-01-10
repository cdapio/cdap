/*
 * Copyright © 2022 Cask Data, Inc.
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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuditLogRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AttributeKey;
import java.util.ArrayDeque;
import java.util.Queue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AuthenticationChannelHandlerTest {

  private DefaultHttpRequest req;
  private AuthenticationChannelHandler handler;
  private ChannelHandlerContext ctx;


  @Before
  public void initHandler() {
    boolean internalAuthEnabled = true;
    handler = new AuthenticationChannelHandler(internalAuthEnabled, false, null);
    ctx = mock(ChannelHandlerContext.class, RETURNS_DEEP_STUBS);
    req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "foo");
  }

  @Test(expected = UnauthenticatedException.class)
  public void testEmptyHeaderThrowsUnauthenticatedException() throws Exception {
    req.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, "");

    handler.channelRead(ctx, req);
  }

  @Test(expected = UnauthenticatedException.class)
  public void testMalformedInvalidCredentialThrows() throws Exception {
    req
      .headers()
      .set(Constants.Security.Headers.RUNTIME_TOKEN, Credential.CredentialType.EXTERNAL_BEARER.getQualifiedName());

    handler.channelRead(ctx, req);
  }

  @Test(expected = UnauthenticatedException.class)
  public void testMalformedValidCredentialThrows() throws Exception {
    req
      .headers()
      .set(Constants.Security.Headers.RUNTIME_TOKEN, Credential.CredentialType.INTERNAL.getQualifiedName());

    handler.channelRead(ctx, req);
  }

  @Test(expected = UnauthenticatedException.class)
  public void testWellFormedInvalidCredentialThrows() throws Exception {
    req
      .headers()
      .set(Constants.Security.Headers.RUNTIME_TOKEN,
           Credential.CredentialType.EXTERNAL_BEARER.getQualifiedName() + " token");

    handler.channelRead(ctx, req);
  }

  @Test
  public void testWellFormedValidCredentialCallsFireChannelReader() throws Exception {
    req
      .headers()
      .set(Constants.Security.Headers.RUNTIME_TOKEN, Credential.CredentialType.INTERNAL.getQualifiedName() + " token");

    handler.channelRead(ctx, req);
    verify(ctx, times(1)).fireChannelRead(any());
  }


  /**
   * This simulates the order for NamespaceHttpHandler#create
   * The flow :
   *  ACH.channelRead.fireChannelRead
   *   ->NamespaceHttpHandler#create (sets AuditLogQueue in SRC)
   *     -> AuditLogSetterHook (Generate metadata and set in SRC's Auditlogrequest.Builder )
   *       -> ACH#readChannel's Finally block (Sets the AuditLogQueue, Auditlogrequest.Builder in Attribute of channel)
   *         -> AuthChannelHandler#write (creates the AuditLogRequest from SRC or ATTR and publishes)
   */
  @Test
  public void testCallOrderCreateNamespaceForAuditLog() throws Exception {
    req
      .headers()
      .set(Constants.Security.Headers.RUNTIME_TOKEN, Credential.CredentialType.INTERNAL.getQualifiedName() + " token");
    AuditLogWriter auditLogWriterMock = Mockito.mock(AuditLogWriter.class);
    handler = new AuthenticationChannelHandler(true, true, auditLogWriterMock);

    //The ACH.channelRead.fireChannelRead , will trigger NamespaceHttpHandler and AuditLogSetterHook
    // So set AuditLogQueue and MetaData in SRC to simulate that.
    Mockito.when(ctx.fireChannelRead(any())).thenAnswer(invocation -> {
      SecurityRequestContext.enqueueAuditLogContext(getAuditLogContexts());
      SecurityRequestContext.setAuditLogRequestBuilder(getAuditLogRequestBuilder());
      return null;
    });

    // ACH.channelRead.fireChannelRead
    // This should add AuditLogContextsQueue and AuditLogRequest.Builder in SRC.
    // and in Finally it should have these in ATTRS
    handler.channelRead(ctx, req);

    //ENUSRE THAT ATTR WAS SET in ACH's Finally Block.
    verify(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_CONTEXT_QUEUE_ATTR)),
      times(1)).set(any());
    verify(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_REQ_BUILDER_ATTR)),
          times(1)).set(any());

    // Now in Write and getAuditLogRequest , should create AuditLogRequest properly from ATTRs
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_CONTEXT_QUEUE_ATTR))
                   .get()).thenReturn(getAuditLogContexts());
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_USER_IP_ATTR)).get())
      .thenReturn("testuserIp");
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_REQ_BUILDER_ATTR))
                   .get()).thenReturn(getAuditLogRequestBuilder());

    handler.write(ctx, "msg", new DefaultChannelPromise(ctx.channel()));
    verify(auditLogWriterMock, times(1)).publish(any());
  }

  /**
   * This simulates the order for ArtifactHttpHandler#addArtifact
   * The flow :
   *  ACH.channelRead.fireChannelRead
   *   ->ArtifactHttpHandler#addArtifact (sets AuditLogQueue in SRC)
   *     -> ACH#readChannel's Finally block (Sets ONLY the AuditLogQueue from SRC in ATTR)
   *       [Here AuditLogBuilder in channel's attribute Should be Null.]
   *       -> 1000+ calls happen via channelRead as upload happens in chunks [ Nothing to mock here ]
   *        ->  AuditLogSetterHook (Generate metadata and set in SRC's Auditlogrequest.Builder )
   *            [This COULD ON DIFFERENT THREAD, this is the last call after Upload is complete]
   *          -> ACH#readChannel's Finally block (Sets ONLY the Auditlogrequest.Builder from SRC in ATTR)
   *            -> AuthChannelHandler#write (creates the AuditLogRequest from SRC or ATTR and publishes)
   */
  @Test
  public void testCallOrderAddArtifactForAuditLog() throws Exception {
    req
      .headers()
      .set(Constants.Security.Headers.RUNTIME_TOKEN, Credential.CredentialType.INTERNAL.getQualifiedName() + " token");
    AuditLogWriter auditLogWriterMock = Mockito.mock(AuditLogWriter.class);
    handler = new AuthenticationChannelHandler(true, true, auditLogWriterMock);

    //The ACH.channelRead.fireChannelRead , will trigger artifactHttpHandler and NOT AuditLogSetterHook
    // So set AuditLogQueue in SRC to simulate that.
    Mockito.when(ctx.fireChannelRead(any())).thenAnswer(invocation -> {
      SecurityRequestContext.enqueueAuditLogContext(getAuditLogContexts());
      return null;
    });

    // ACH.channelRead.fireChannelRead
    // This should add AuditLogContextsQueue and AuditLogRequest.Builder in SRC.
    // and in Finally it should have these in ATTRS
    handler.channelRead(ctx, req);

    //ENUSRE THAT ATTR WAS SET in ACH's Finally Block.
    verify(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_CONTEXT_QUEUE_ATTR)),
           times(1)).set(any());
    // ENSURE AuditLogRequest Builder is NOT SET
    verify(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_REQ_BUILDER_ATTR)),
           times(0)).set(any());

    //Now in a different Thread , when the UPLOAD is finally complete, it reaches AuditLogSetterHook and sets only
    // the Metadata / Builder
    Mockito.when(ctx.fireChannelRead(any())).thenAnswer(invocation -> {
      SecurityRequestContext.setAuditLogRequestBuilder(getAuditLogRequestBuilder());
      return null;
    });

    //The final read call which invokes AuditLogSetterHook
    handler.channelRead(ctx, req);

    //ENUSRE THAT auditLogContextQueue was NOT SET i.e. ( total overall 1 call )
    verify(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_CONTEXT_QUEUE_ATTR)),
           times(1)).set(any());
    // ENSURE AuditLogRequest Builder is SET
    verify(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_REQ_BUILDER_ATTR)),
           times(1)).set(any());

    // Now in Write and getAuditLogRequest , should create AuditLogRequest properly from ATTRs
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_CONTEXT_QUEUE_ATTR))
                   .get()).thenReturn(getAuditLogContexts());
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_USER_IP_ATTR)).get())
      .thenReturn("testuserIp");
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_REQ_BUILDER_ATTR))
                   .get()).thenReturn(getAuditLogRequestBuilder());

    handler.write(ctx, "msg", new DefaultChannelPromise(ctx.channel()));
    verify(auditLogWriterMock, times(1)).publish(any());
  }

  @Test
  public void testWriteWithAuditLogging() throws Exception {
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_CONTEXT_QUEUE_ATTR))
                   .get()).thenReturn(getAuditLogContexts());
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_USER_IP_ATTR)).get())
      .thenReturn("testuserIp");
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_REQ_BUILDER_ATTR))
                   .get()).thenReturn(getAuditLogRequestBuilder());
    AuditLogWriter auditLogWriterMock = Mockito.mock(AuditLogWriter.class);
    handler = new AuthenticationChannelHandler(true, true, auditLogWriterMock);
    handler.write(ctx, "msg", new DefaultChannelPromise(ctx.channel()));

    verify(auditLogWriterMock, times(1)).publish(any());
  }

  @Test
  public void testCloseWithAuditLogging() throws Exception {
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_CONTEXT_QUEUE_ATTR))
                   .get()).thenReturn(getAuditLogContexts());
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_USER_IP_ATTR)).get())
      .thenReturn("testuserIp");
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AuthenticationChannelHandler.AUDIT_LOG_REQ_BUILDER_ATTR))
                   .get()).thenReturn(getAuditLogRequestBuilder());
    AuditLogWriter auditLogWriterMock = Mockito.mock(AuditLogWriter.class);
    handler = new AuthenticationChannelHandler(true, true, auditLogWriterMock);
    handler.close(ctx, new DefaultChannelPromise(ctx.channel()));

    verify(auditLogWriterMock, times(1)).publish(any());
  }

  private ChannelHandlerContext setAuditLogsInFireChannelRead() {
    SecurityRequestContext.enqueueAuditLogContext(getAuditLogContexts());
    return null;
  }

  private Queue<AuditLogContext> getAuditLogContexts() {
    Queue<AuditLogContext> auditLogContexts = new ArrayDeque<>();
    auditLogContexts.add(AuditLogContext.Builder.defaultNotRequired());
    auditLogContexts.add(new AuditLogContext.Builder()
                           .setAuditLoggingRequired(true)
                           .setAuditLogBody("Test Audit Logs")
                           .build());
    return auditLogContexts;
  }

  private AuditLogRequest.Builder getAuditLogRequestBuilder() {
    return new AuditLogRequest.Builder()
      .operationResponseCode(200)
      .uri("v3/test")
      .handler("Testhandler")
      .method("create")
      .methodType("POST")
      .startTimeNanos(1000000L)
      .endTimeNanos(1000002L);
  }
}
