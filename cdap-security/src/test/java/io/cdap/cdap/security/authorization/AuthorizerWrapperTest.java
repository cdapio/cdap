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

package io.cdap.cdap.security.authorization;

import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.spi.authorization.Authorizer;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AuthorizerWrapper}.
 */
public class AuthorizerWrapperTest {

  @Test
  public void testInitializeAndDestroySuccess() throws Exception {
    Authorizer authorizer = Mockito.mock(Authorizer.class);
    AuthorizationContext context = Mockito.mock(AuthorizationContext.class);
    AuthorizerWrapper wrapper = new AuthorizerWrapper(authorizer);

    wrapper.initialize(context);
    wrapper.destroy();

    Mockito.verify(authorizer).initialize(context);
    Mockito.verify(authorizer).destroy();
  }

  @Test
  public void testInitializePropagatesUncheckedDirectlyAndWrapsChecked() throws Exception {
    AuthorizationContext context = Mockito.mock(AuthorizationContext.class);

    Authorizer uncheckedAuthorizer = Mockito.mock(Authorizer.class);
    IllegalStateException expectedUnchecked = new IllegalStateException("unchecked init error");
    Mockito.doThrow(expectedUnchecked).when(uncheckedAuthorizer).initialize(context);
    AuthorizerWrapper uncheckedWrapper = new AuthorizerWrapper(uncheckedAuthorizer);
    try {
      uncheckedWrapper.initialize(context);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException caught) {
      Assert.assertSame(expectedUnchecked, caught);
    }

    Authorizer checkedAuthorizer = Mockito.mock(Authorizer.class);
    IOException expectedChecked = new IOException("checked init error");
    Mockito.doThrow(expectedChecked).when(checkedAuthorizer).initialize(context);
    AuthorizerWrapper checkedWrapper = new AuthorizerWrapper(checkedAuthorizer);
    try {
      checkedWrapper.initialize(context);
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException caught) {
      Assert.assertSame(expectedChecked, caught.getCause());
    }
  }

  @Test
  public void testDestroyPropagatesUncheckedDirectlyAndWrapsChecked() throws Exception {
    Authorizer uncheckedAuthorizer = Mockito.mock(Authorizer.class);
    IllegalArgumentException expectedUnchecked = new IllegalArgumentException("unchecked destroy error");
    Mockito.doThrow(expectedUnchecked).when(uncheckedAuthorizer).destroy();
    AuthorizerWrapper uncheckedWrapper = new AuthorizerWrapper(uncheckedAuthorizer);
    try {
      uncheckedWrapper.destroy();
      Assert.fail("Expected IllegalArgumentException");
    } catch (RuntimeException caught) {
      Assert.assertSame(expectedUnchecked, caught);
    }

    Authorizer checkedAuthorizer = Mockito.mock(Authorizer.class);
    Exception expectedChecked = new Exception("checked destroy error");
    Mockito.doThrow(expectedChecked).when(checkedAuthorizer).destroy();
    AuthorizerWrapper checkedWrapper = new AuthorizerWrapper(checkedAuthorizer);
    try {
      checkedWrapper.destroy();
      Assert.fail("Expected RuntimeException wrapping checked Exception");
    } catch (RuntimeException caught) {
      Assert.assertSame(expectedChecked, caught.getCause());
    }
  }
}
