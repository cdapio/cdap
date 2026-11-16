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

package io.cdap.cdap.data2.dataset2;

import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.common.DatasetNotFoundException;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DatasetExistenceVerifier}.
 */
public class DatasetExistenceVerifierTest {

  @Test
  public void testEnsureExistsSuccessNotFoundAndManagementExceptionPropagation() throws Exception {
    DatasetId existingId = NamespaceId.DEFAULT.dataset("existing");
    DatasetId missingId = NamespaceId.DEFAULT.dataset("missing");
    DatasetId errorId = NamespaceId.DEFAULT.dataset("error");
    final DatasetManagementException expectedDme =
        new DatasetManagementException("backend check failed");

    DatasetFramework fw = Mockito.mock(DatasetFramework.class);
    Mockito.when(fw.hasInstance(existingId)).thenReturn(true);
    Mockito.when(fw.hasInstance(missingId)).thenReturn(false);
    Mockito.when(fw.hasInstance(errorId)).thenThrow(expectedDme);

    DatasetExistenceVerifier verifier = new DatasetExistenceVerifier(fw);

    verifier.ensureExists(existingId);

    try {
      verifier.ensureExists(missingId);
      Assert.fail("Expected DatasetNotFoundException");
    } catch (DatasetNotFoundException expected) {
      Assert.assertEquals(missingId, expected.getId());
    }

    try {
      verifier.ensureExists(errorId);
      Assert.fail("Expected RuntimeException wrapping DatasetManagementException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedDme, actual.getCause());
    }
  }
}
