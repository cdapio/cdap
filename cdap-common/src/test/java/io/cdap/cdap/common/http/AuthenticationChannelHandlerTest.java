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
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AttributeKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.Queue;

public class AuthenticationChannelHandlerTest {

  private DefaultHttpRequest req;
  private AuthenticationChannelHandler handler;
  private ChannelHandlerContext ctx;
  private static final String AUDIT_LOG_QUEUE_ATTR_NAME = "AUDIT_LOG_QUEUE";


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

  @Test
  public void testWriteWithAuditLogging() throws Exception {
    boolean internalAuthEnabled = true;
    AuditLogWriter auditLogWriterMock = Mockito.mock(AuditLogWriter.class);
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_QUEUE_ATTR_NAME)).get())
      .thenReturn(getAuditLogContexts());
    handler = new AuthenticationChannelHandler(internalAuthEnabled, true, auditLogWriterMock);
    handler.write(ctx, "msg", new DefaultChannelPromise(ctx.channel()));

    verify(auditLogWriterMock, times(1)).publish(any());
  }

  @Test
  public void testCloseWithAuditLogging() throws Exception {
    boolean internalAuthEnabled = true;
    AuditLogWriter auditLogWriterMock = Mockito.mock(AuditLogWriter.class);
    Mockito.when(ctx.channel().attr(AttributeKey.valueOf(AUDIT_LOG_QUEUE_ATTR_NAME)).get())
      .thenReturn(getAuditLogContexts());
    handler = new AuthenticationChannelHandler(internalAuthEnabled, true, auditLogWriterMock);
    handler.close(ctx, new DefaultChannelPromise(ctx.channel()));

    verify(auditLogWriterMock, times(1)).publish(any());
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
}
