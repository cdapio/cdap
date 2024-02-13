/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.net.URL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;



@RunWith(PowerMockRunner.class)
@PrepareForTest({DataprocUtils.class})
/*
 * Unit tests for {@link DataprocUtils}.
 */
public class DataprocUtilsTest {

  @Test
  public void testParseSingleLabel() {
    Map<String, String> expected = new HashMap<>();
    expected.put("key", "val");
    assertEquals(expected, DataprocUtils.parseLabels("key=val", ",", "="));
  }

  @Test
  public void testParseMultipleLabels() {
    Map<String, String> expected = new HashMap<>();
    expected.put("k1", "v1");
    expected.put("k2", "v2");
    assertEquals(expected,
                 DataprocUtils.parseLabels("k1=v1,k2=v2", ",", "="));
  }

  @Test
  public void testParseLabelsIgnoresWhitespace() {
    Map<String, String> expected = new HashMap<>();
    expected.put("k1", "v1");
    expected.put("k2", "v2");
    assertEquals(expected,
                 DataprocUtils.parseLabels(" k1  =\tv1  ,\nk2 = v2  ", ",", "="));
  }

  @Test
  public void testParseLabelsWithoutVal() {
    Map<String, String> expected = new HashMap<>();
    expected.put("k1", "");
    expected.put("k2", "");
    assertEquals(expected, DataprocUtils.parseLabels("k1,k2=", ",", "="));
  }

  @Test
  public void testParseLabelsIgnoresInvalidKey() {
    Map<String, String> expected = new HashMap<>();
    assertEquals(expected, DataprocUtils.parseLabels(("A"), ",", "="));
    assertEquals(expected, DataprocUtils.parseLabels(("0"), ",", "="));
    assertEquals(expected, DataprocUtils.parseLabels(("a.b"), ",", "="));
    assertEquals(expected, DataprocUtils.parseLabels(("a^b"), ",", "="));
    StringBuilder longStr = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      longStr.append('a');
    }
    assertEquals(expected, DataprocUtils.parseLabels(longStr.toString(), ",", "="));
  }

  @Test
  public void testParseLabelsIgnoresInvalidVal() {
    Map<String, String> expected = new HashMap<>();
    assertEquals(expected, DataprocUtils.parseLabels("a=A", ",", "="));
    assertEquals(expected, DataprocUtils.parseLabels("a=ab.c", ",", "="));
    StringBuilder longStr = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      longStr.append('a');
    }
    assertEquals(expected, DataprocUtils.parseLabels(String.format("a=%s", longStr.toString()), ",", "="));
  }

  @Test
  public void testGetMetadataSuccessOnFirstAttempt() throws Exception {
    // Arrange
    String expectedMetadata = "Sample metadata";
    HttpURLConnection mockConnection = mock(HttpURLConnection.class);
    when(mockConnection.getInputStream()).thenReturn(new ByteArrayInputStream(expectedMetadata.getBytes()));

    URL mockUrl = PowerMockito.mock(URL.class);
    PowerMockito.whenNew(URL.class).withAnyArguments().thenReturn(mockUrl);
    when(mockUrl.openConnection()).thenReturn(mockConnection);

    String zone = DataprocUtils.getSystemZone();
    assertEquals(expectedMetadata, zone);

    verify(mockConnection).setRequestProperty("Metadata-Flavor", "Google");
    verify(mockConnection).connect();
    verify(mockConnection).disconnect();
  }

  @Test
  public void testGetMetadataFailureOnFirstAttempt() throws Exception {
    HttpURLConnection mockConnection = mock(HttpURLConnection.class);
    when(mockConnection.getResponseCode()).thenReturn(403);
    when(mockConnection.getInputStream()).thenThrow(new IOException("Test Exception"));

    URL mockUrl = PowerMockito.mock(URL.class);
    PowerMockito.whenNew(URL.class).withAnyArguments().thenReturn(mockUrl);
    when(mockUrl.openConnection()).thenReturn(mockConnection);

    // Act
    Exception exception = null;
    try {
      DataprocUtils.getSystemZone();
    } catch (Exception e) {
      exception = e;
    }

    // Assert
    verify(mockConnection, times(1)).setRequestProperty("Metadata-Flavor", "Google");
    verify(mockConnection, times(1)).connect();
    verify(mockConnection, times(1)).disconnect();
    Assert.assertTrue(exception instanceof RuntimeException);
  }

  @Test
  public void testGetMetadataSuccessAfterRetries() throws Exception {
    String expectedMetadata = "Sample metadata";
    HttpURLConnection mockConnection = mock(HttpURLConnection.class);
    when(mockConnection.getResponseCode()).thenReturn(500).thenReturn(500).thenReturn(200);
    when(mockConnection.getInputStream()).thenThrow(new IOException("Test Exception")).thenThrow(new IOException("Test Exception")).thenReturn(new ByteArrayInputStream(expectedMetadata.getBytes()));

    URL mockUrl = PowerMockito.mock(URL.class);
    PowerMockito.whenNew(URL.class).withAnyArguments().thenReturn(mockUrl);
    when(mockUrl.openConnection()).thenReturn(mockConnection);

    String zone = DataprocUtils.getSystemZone();
    assertEquals(expectedMetadata, zone);

    verify(mockConnection, times(3)).setRequestProperty("Metadata-Flavor", "Google");
    verify(mockConnection, times(3)).connect();
    verify(mockConnection, times(3)).disconnect();
  }

  @Test
  public void testGetMetadataMaxRetriesExceeded() throws Exception {
    // Arrange
    HttpURLConnection mockConnection = mock(HttpURLConnection.class);
    when(mockConnection.getResponseCode()).thenReturn(500);
    when(mockConnection.getInputStream()).thenThrow(new IOException("Test Exception"));

    URL mockUrl = PowerMockito.mock(URL.class);
    PowerMockito.whenNew(URL.class).withAnyArguments().thenReturn(mockUrl);
    when(mockUrl.openConnection()).thenReturn(mockConnection);

    // Act
    Exception exception = null;
    try {
      DataprocUtils.getSystemZone();
    } catch (Exception e) {
      exception = e;
    }

    // Assert
    verify(mockConnection, times(5)).setRequestProperty("Metadata-Flavor", "Google");
    verify(mockConnection, times(5)).connect();
    verify(mockConnection, times(5)).disconnect();
    Assert.assertTrue(exception instanceof RuntimeException);
  }
}
