/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.common;

import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link HttpExceptionHandler}.
 */
public class HttpExceptionHandlerTest {

  private String previousUserId;

  @Before
  public void setUp() {
    previousUserId = SecurityRequestContext.getUserId();
  }

  @After
  public void tearDown() {
    SecurityRequestContext.setUserId(previousUserId);
  }

  @Test
  public void testUnexpectedExceptionWithNullAndNonNullUserId() {
    HttpExceptionHandler handler = new HttpExceptionHandler();
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v3/apps");
    Throwable nested = new RuntimeException("wrapper", new IllegalStateException("root failure"));

    SecurityRequestContext.setUserId(null);
    HttpResponder responderNullUser = Mockito.mock(HttpResponder.class);
    handler.handle(nested, request, responderNullUser);
    Mockito.verify(responderNullUser)
        .sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "root failure");

    SecurityRequestContext.setUserId("alice");
    HttpResponder responderWithUser = Mockito.mock(HttpResponder.class);
    handler.handle(nested, request, responderWithUser);
    Mockito.verify(responderWithUser)
        .sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "root failure");
  }

  @Test
  public void testKnownExceptionsWithNullAndNonNullUserId() {
    HttpExceptionHandler handler = new HttpExceptionHandler();
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v3/ns");

    for (String userId : new String[]{null, "bob"}) {
      SecurityRequestContext.setUserId(userId);

      HttpResponder unavailableResponder = Mockito.mock(HttpResponder.class);
      handler.handle(new RuntimeException(new ServiceUnavailableException("masterService")),
          request, unavailableResponder);
      Mockito.verify(unavailableResponder).sendString(
          Mockito.eq(HttpResponseStatus.SERVICE_UNAVAILABLE), Mockito.anyString());

      HttpResponder customStatusResponder = Mockito.mock(HttpResponder.class);
      handler.handle(new RuntimeException(new CustomStatusException("bad input", 400)),
          request, customStatusResponder);
      Mockito.verify(customStatusResponder)
          .sendString(HttpResponseStatus.BAD_REQUEST, "bad input");

      HttpResponder notFoundResponder = Mockito.mock(HttpResponder.class);
      handler.handle(new SampleNotFoundException("missing entity"), request, notFoundResponder);
      Mockito.verify(notFoundResponder)
          .sendString(HttpResponseStatus.NOT_FOUND, "missing entity");

      HttpResponder conflictResponder = Mockito.mock(HttpResponder.class);
      handler.handle(new SampleAlreadyExistsException("duplicate entity"),
          request, conflictResponder);
      Mockito.verify(conflictResponder)
          .sendString(HttpResponseStatus.CONFLICT, "duplicate entity");

      HttpResponder unsupportedResponder = Mockito.mock(HttpResponder.class);
      handler.handle(new UnsupportedOperationException("not supported"),
          request, unsupportedResponder);
      Mockito.verify(unsupportedResponder)
          .sendString(HttpResponseStatus.NOT_IMPLEMENTED, "not supported");
    }
  }

  private static final class CustomStatusException extends Exception
      implements HttpErrorStatusProvider {
    private final int statusCode;

    CustomStatusException(String message, int statusCode) {
      super(message);
      this.statusCode = statusCode;
    }

    @Override
    public int getStatusCode() {
      return statusCode;
    }
  }

  private static final class SampleNotFoundException extends Exception {
    SampleNotFoundException(String message) {
      super(message);
    }
  }

  private static final class SampleAlreadyExistsException extends Exception {
    SampleAlreadyExistsException(String message) {
      super(message);
    }
  }
}
