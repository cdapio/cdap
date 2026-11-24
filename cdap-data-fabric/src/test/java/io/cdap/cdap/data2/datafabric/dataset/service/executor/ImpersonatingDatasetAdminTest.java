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

package io.cdap.cdap.data2.datafabric.dataset.service.executor;

import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ImpersonatingDatasetAdmin} exception propagation.
 */
public class ImpersonatingDatasetAdminTest {

  @Test
  public void testExecutePropagatesIoAndUncheckedDirectlyAndWrapsCheckedException()
      throws Exception {
    final DatasetId datasetId = NamespaceId.DEFAULT.dataset("impDs");
    DatasetAdmin delegate = Mockito.mock(DatasetAdmin.class);
    Mockito.when(delegate.exists()).thenReturn(true);

    Impersonator impersonator = Mockito.mock(Impersonator.class);
    final IOException expectedIo = new IOException("io failure");
    final IllegalStateException expectedRuntime = new IllegalStateException("unchecked failure");
    final Exception expectedChecked = new Exception("checked non-io failure");

    Mockito.when(impersonator.doAs(Mockito.eq(datasetId), Mockito.any(Callable.class)))
        .thenAnswer(inv -> inv.getArgument(1, Callable.class).call())
        .thenThrow(expectedIo)
        .thenThrow(expectedRuntime)
        .thenThrow(expectedChecked);

    ImpersonatingDatasetAdmin admin =
        new ImpersonatingDatasetAdmin(delegate, impersonator, datasetId);

    // 1. Happy path delegates cleanly
    Assert.assertTrue(admin.exists());

    // 2. IOException propagates directly
    try {
      admin.create();
      Assert.fail("Expected IOException");
    } catch (IOException actual) {
      Assert.assertSame(expectedIo, actual);
    }

    // 3. Unchecked RuntimeException propagates directly without double-wrapping
    try {
      admin.drop();
      Assert.fail("Expected exact IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    // 4. Other checked Exception wraps in RuntimeException
    try {
      admin.truncate();
      Assert.fail("Expected RuntimeException wrapping AccessException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedChecked, actual.getCause());
    }
  }
}
