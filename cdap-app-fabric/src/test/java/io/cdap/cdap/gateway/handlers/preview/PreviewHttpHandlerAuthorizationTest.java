/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.preview;

import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests authorization of {@link PreviewHttpHandler} endpoints.
 */
public class PreviewHttpHandlerAuthorizationTest {

  @Test
  public void testGetTracers() throws Exception {
    PreviewManager previewManager = Mockito.mock(PreviewManager.class);
    Mockito.when(previewManager.getStatus(new ApplicationId("ns", "denied")))
        .thenThrow(new UnauthorizedException("denied"));
    PreviewHttpHandler handler = new PreviewHttpHandler(previewManager, CConfiguration.create());

    HttpResponder responder = Mockito.mock(HttpResponder.class);
    try {
      handler.getTracers(null, responder, "ns", "denied");
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    Mockito.verifyNoInteractions(responder);

    handler.getTracers(null, responder, "ns", "allowed");
    Mockito.verify(previewManager).getStatus(new ApplicationId("ns", "allowed"));
    Mockito.verify(responder).sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }
}
