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

package io.cdap.cdap.security.impersonation;

import io.cdap.cdap.proto.NamespaceMeta;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ImpersonationUtils}.
 */
public class ImpersonationUtilsTest {

  private static final class CustomCheckedException extends Exception {
    CustomCheckedException(String message) {
      super(message);
    }
  }

  private static final class CustomDirectThrowable extends Throwable {
    CustomDirectThrowable(String message) {
      super(message);
    }
  }

  @Test
  public void testDoAsSuccessAndImpersonatingCallable() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("alice");
    String actualUser = ImpersonationUtils.doAs(ugi, new Callable<String>() {
      @Override
      public String call() throws Exception {
        return UserGroupInformation.getCurrentUser().getShortUserName();
      }
    });
    Assert.assertEquals("alice", actualUser);

    Impersonator impersonator = Mockito.mock(Impersonator.class);
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName("ns1").build();
    Callable<String> inner = new Callable<String>() {
      @Override
      public String call() {
        return "done";
      }
    };
    Mockito.when(impersonator.doAs(nsMeta.getNamespaceId(), inner)).thenReturn("done");
    Assert.assertEquals("done", ImpersonationUtils.createImpersonatingCallable(impersonator, nsMeta, inner).call());
  }

  @Test
  public void testDoAsUnwrapsCheckedExceptionFromUgi() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("alice");
    final CustomCheckedException expected = new CustomCheckedException("checked failure");
    try {
      ImpersonationUtils.doAs(ugi, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          throw expected;
        }
      });
      Assert.fail("Expected CustomCheckedException");
    } catch (CustomCheckedException caught) {
      Assert.assertSame(expected, caught);
    }
  }

  @Test
  public void testDoAsUnwrapsRuntimeErrorIoAndDirectThrowableFromUndeclaredThrowableException() throws Exception {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser("alice");

    final IllegalStateException expectedRuntime = new IllegalStateException("wrapped runtime");
    try {
      ImpersonationUtils.doAs(ugi, new Callable<Void>() {
        @Override
        public Void call() {
          throw new UndeclaredThrowableException(expectedRuntime);
        }
      });
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException caught) {
      Assert.assertSame(expectedRuntime, caught);
    }

    final AssertionError expectedError = new AssertionError("wrapped error");
    try {
      ImpersonationUtils.doAs(ugi, new Callable<Void>() {
        @Override
        public Void call() {
          throw new UndeclaredThrowableException(expectedError);
        }
      });
      Assert.fail("Expected AssertionError");
    } catch (AssertionError caught) {
      Assert.assertSame(expectedError, caught);
    }

    final IOException expectedIo = new IOException("wrapped io");
    try {
      ImpersonationUtils.doAs(ugi, new Callable<Void>() {
        @Override
        public Void call() {
          throw new UndeclaredThrowableException(expectedIo);
        }
      });
      Assert.fail("Expected IOException");
    } catch (IOException caught) {
      Assert.assertSame(expectedIo, caught);
    }

    final CustomDirectThrowable expectedThrowable = new CustomDirectThrowable("direct throwable");
    try {
      ImpersonationUtils.doAs(ugi, new Callable<Void>() {
        @Override
        public Void call() {
          throw new UndeclaredThrowableException(expectedThrowable);
        }
      });
      Assert.fail("Expected RuntimeException wrapping CustomDirectThrowable");
    } catch (RuntimeException caught) {
      Assert.assertSame(expectedThrowable, caught.getCause());
    }
  }
}
