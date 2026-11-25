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

package io.cdap.cdap.data2.dataset2.lib.partitioned;

import io.cdap.cdap.api.dataset.DataSetException;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.IndexedTable;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.data2.dataset2.lib.file.FileSetDataset;
import io.cdap.cdap.proto.id.NamespaceId;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link PartitionedFileSetDataset#onFailure()} exception propagation.
 */
public class PartitionedFileSetDatasetOnFailureTest {

  @Test
  public void testOnFailurePropagatesDataSetAndRuntimeExceptionsAndWrapsCheckedThrowable()
      throws Exception {
    CConfiguration cConf = CConfiguration.create();
    DatasetContext context = DatasetContext.from(NamespaceId.DEFAULT.getNamespace());
    DatasetSpecification spec = DatasetSpecification.builder("pfs", "partitionedFileSet").build();
    Partitioning partitioning = Partitioning.builder().addStringField("dt").build();

    LocationFactory lf = Mockito.mock(LocationFactory.class);
    Location nsLoc = Mockito.mock(Location.class);
    Location dataLoc = Mockito.mock(Location.class);
    Location baseLoc = Mockito.mock(Location.class);
    Location outLoc = Mockito.mock(Location.class);

    Mockito.when(nsLoc.append(Mockito.anyString())).thenReturn(dataLoc);
    Mockito.when(dataLoc.append(Mockito.anyString())).thenReturn(baseLoc);
    Mockito.when(baseLoc.getLocationFactory()).thenReturn(lf);
    Mockito.when(baseLoc.append("partOut")).thenReturn(outLoc);
    Mockito.when(outLoc.toURI()).thenReturn(URI.create("file:///tmp/partOut"));

    NamespacePathLocator pathLocator = Mockito.mock(NamespacePathLocator.class);
    Mockito.when(pathLocator.get(NamespaceId.DEFAULT)).thenReturn(nsLoc);

    Map<String, String> runtimeArgs = new HashMap<>();
    FileSetArguments.setOutputPath(runtimeArgs, "partOut");

    FileSetDataset embeddedFileSet =
        new FileSetDataset(context, cConf, spec, lf, pathLocator, runtimeArgs);
    IndexedTable partitionsTable = Mockito.mock(IndexedTable.class);
    PartitionedFileSetDataset pfs = new PartitionedFileSetDataset(
        context, "pfs", partitioning, embeddedFileSet, partitionsTable, spec, runtimeArgs);

    final DataSetException expectedDataSetEx = new DataSetException("dataset failure");
    final IllegalStateException expectedRuntimeEx = new IllegalStateException("unchecked failure");
    final Exception expectedCheckedEx = new Exception("checked rollback failure");

    Mockito.when(outLoc.isDirectory())
        .thenThrow(expectedDataSetEx)
        .thenThrow(expectedRuntimeEx)
        .thenAnswer(inv -> {
          throw expectedCheckedEx;
        });

    try {
      pfs.onFailure();
      Assert.fail("Expected DataSetException to propagate directly");
    } catch (DataSetException actual) {
      Assert.assertSame(expectedDataSetEx, actual);
    }

    try {
      pfs.onFailure();
      Assert.fail("Expected unchecked RuntimeException to propagate directly");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntimeEx, actual);
    }

    try {
      pfs.onFailure();
      Assert.fail("Expected checked exception to be wrapped in DataSetException");
    } catch (DataSetException actual) {
      Assert.assertSame(expectedCheckedEx, actual.getCause());
    }
  }
}
