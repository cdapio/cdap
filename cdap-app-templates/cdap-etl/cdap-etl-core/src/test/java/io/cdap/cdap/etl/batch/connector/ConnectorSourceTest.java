/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.connector;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import java.io.IOException;
import org.apache.twill.filesystem.Location;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for the {@link ConnectorSource} class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ConnectorSourceTest {

  private static final String TEST_DATASET_NAME = "testConnectorData";

  @Mock
  private BatchSourceContext mockContext;

  @Mock
  private FileSet mockFileSet;

  @Mock
  private Location mockLocation;

  private ConnectorSource<Object> connectorSource;

  @Before
  public void setUp() {
    connectorSource = new ConnectorSource<>(TEST_DATASET_NAME);
  }

  @Test
  public void testOnRunFinish_whenRunSucceeds_shouldDeleteLocation() throws IOException {
    when(mockContext.getDataset(TEST_DATASET_NAME)).thenReturn(mockFileSet);
    when(mockFileSet.getBaseLocation()).thenReturn(mockLocation);
    when(mockLocation.exists()).thenReturn(true);
    when(mockLocation.delete(true)).thenReturn(true);
    connectorSource.onRunFinish(true, mockContext);

    verify(mockContext).getDataset(TEST_DATASET_NAME);
    verify(mockLocation).delete(true);
  }

  @Test
  public void testOnRunFinish_whenBaseLocationDoesNotExist_shouldSkipDelete() throws IOException {
    when(mockContext.getDataset(TEST_DATASET_NAME)).thenReturn(mockFileSet);
    when(mockFileSet.getBaseLocation()).thenReturn(mockLocation);
    when(mockLocation.exists()).thenReturn(false);
    connectorSource.onRunFinish(true, mockContext);

    verify(mockLocation, never()).delete(true);
  }

  @Test
  public void testOnRunFinish_whenBaseLocationIsNull_shouldSkipDelete() {
    when(mockContext.getDataset(TEST_DATASET_NAME)).thenReturn(mockFileSet);
    when(mockFileSet.getBaseLocation()).thenReturn(null);
    connectorSource.onRunFinish(true, mockContext);

    verify(mockFileSet).getBaseLocation();
  }

  @Test
  public void testOnRunFinish_whenDeletionFails_shouldLogWarningAndNotThrow() throws IOException {
    when(mockContext.getDataset(TEST_DATASET_NAME)).thenReturn(mockFileSet);
    when(mockFileSet.getBaseLocation()).thenReturn(mockLocation);
    when(mockLocation.exists()).thenReturn(true);
    when(mockLocation.delete(true)).thenReturn(false);
    connectorSource.onRunFinish(true, mockContext);

    verify(mockLocation).delete(true);
  }
}
