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

package io.cdap.cdap.internal.app.store;

import com.google.common.collect.ImmutableSet;
import com.google.gson.stream.MalformedJsonException;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.app.store.ApplicationFilter;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.store.StoreDefinition;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AppMetadataStore} {@code AppScanEntry.getArtifactId()} and artifact filtering.
 */
public class AppScanEntryTest {

  @Test
  public void testGetArtifactIdCachesParsedValueAndHandlesMissingArtifactId() throws Exception {
    String validJson = "{\"id\":\"app1\",\"spec\":{\"name\":\"app1\","
        + "\"artifactId\":{\"name\":\"my-artifact\",\"version\":{\"version\":\"1.2.3\"},\"scope\":\"USER\"}}}";
    Object entry = createAppScanEntry("app1", validJson);

    Optional<ArtifactId> first = invokeGetArtifactId(entry);
    Optional<ArtifactId> second = invokeGetArtifactId(entry);

    ArtifactId expected = new ArtifactId("my-artifact", new ArtifactVersion("1.2.3"), ArtifactScope.USER);
    Assert.assertTrue(first.isPresent());
    Assert.assertEquals(expected, first.get());
    Assert.assertTrue(second.isPresent());
    Assert.assertSame(first.get(), second.get());

    Object noArtifactEntry = createAppScanEntry("app2", "{\"id\":\"app2\",\"spec\":{\"name\":\"app2\"}}");
    Assert.assertFalse(invokeGetArtifactId(noArtifactEntry).isPresent());

    Object noSpecEntry = createAppScanEntry("app3", "{\"id\":\"app3\"}");
    Assert.assertFalse(invokeGetArtifactId(noSpecEntry).isPresent());
  }

  @Test
  public void testGetArtifactIdMalformedJsonThrowsIllegalStateException() throws Exception {
    Object malformedEntry = createAppScanEntry("appBadJson", "{\"spec\": {malformed");
    try {
      invokeGetArtifactId(malformedEntry);
      Assert.fail("Expected IllegalStateException for malformed JSON");
    } catch (IllegalStateException actual) {
      Assert.assertEquals("Failed to parse artifact ID from app meta", actual.getMessage());
      Assert.assertTrue(actual.getCause() instanceof MalformedJsonException);
    }
  }

  @Test
  public void testGetArtifactIdWrongSpecTypeThrowsIllegalStateException() throws Exception {
    Object wrongTypeEntry = createAppScanEntry("appWrongSpec", "{\"spec\": \"not-an-object\"}");
    try {
      invokeGetArtifactId(wrongTypeEntry);
      Assert.fail("Expected IllegalStateException when spec is not a JSON object");
    } catch (IllegalStateException actual) {
      Assert.assertEquals("Failed to parse artifact ID from app meta", actual.getMessage());
      Assert.assertTrue(actual.getCause() instanceof IllegalStateException);
    }
  }

  @Test
  public void testScanApplicationsWithArtifactIdFilter() throws Exception {
    String matchJson = "{\"id\":\"appMatch\",\"spec\":{\"name\":\"appMatch\","
        + "\"artifactId\":{\"name\":\"target-art\",\"version\":{\"version\":\"2.0.0\"},\"scope\":\"SYSTEM\"}}}";
    String otherJson = "{\"id\":\"appOther\",\"spec\":{\"name\":\"appOther\","
        + "\"artifactId\":{\"name\":\"other-art\",\"version\":{\"version\":\"1.0.0\"},\"scope\":\"USER\"}}}";

    StructuredRow row1 = createMockRow("default", "appMatch", "-SNAPSHOT", matchJson);
    StructuredRow row2 = createMockRow("default", "appOther", "-SNAPSHOT", otherJson);

    StructuredTable appSpecTable = Mockito.mock(StructuredTable.class);
    StructuredTable pluginTable = Mockito.mock(StructuredTable.class);
    StructuredTable universalPluginTable = Mockito.mock(StructuredTable.class);

    Iterator<StructuredRow> rowIter = Arrays.asList(row1, row2).iterator();
    CloseableIterator<StructuredRow> closeableIter = new AbstractCloseableIterator<StructuredRow>() {
      @Override
      protected StructuredRow computeNext() {
        return rowIter.hasNext() ? rowIter.next() : endOfData();
      }

      @Override
      public void close() {
      }
    };
    Mockito.when(appSpecTable.scan(Mockito.any(Range.class), Mockito.anyInt(), Mockito.any(SortOrder.class)))
        .thenReturn(closeableIter);

    StructuredTableContext context = Mockito.mock(StructuredTableContext.class);
    Mockito.when(context.getTable(StoreDefinition.AppMetadataStore.APPLICATION_SPECIFICATIONS))
        .thenReturn(appSpecTable);
    Mockito.when(context.getTable(StoreDefinition.ArtifactStore.PLUGIN_DATA_TABLE))
        .thenReturn(pluginTable);
    Mockito.when(context.getTable(StoreDefinition.ArtifactStore.UNIV_PLUGIN_DATA_TABLE))
        .thenReturn(universalPluginTable);

    AppMetadataStore store = AppMetadataStore.create(context);
    ScanApplicationsRequest request = ScanApplicationsRequest.builder()
        .setNamespaceId(NamespaceId.DEFAULT)
        .addFilter(new ApplicationFilter.ArtifactNamesInFilter(ImmutableSet.of("target-art")))
        .build();

    List<ApplicationId> matchedIds = new ArrayList<>();
    store.scanApplications(request, entry -> {
      matchedIds.add(entry.getKey());
      return true;
    });

    Assert.assertEquals(1, matchedIds.size());
    Assert.assertEquals(NamespaceId.DEFAULT.app("appMatch"), matchedIds.get(0));
  }

  private static StructuredRow createMockRow(
      String namespace, String appName, String version, String rawAppMeta) {
    StructuredRow row = Mockito.mock(StructuredRow.class);
    Mockito.when(row.getString(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD)).thenReturn(namespace);
    Mockito.when(row.getString(StoreDefinition.AppMetadataStore.APPLICATION_FIELD)).thenReturn(appName);
    Mockito.when(row.getString(StoreDefinition.AppMetadataStore.VERSION_FIELD)).thenReturn(version);
    Mockito.when(row.getString(StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD)).thenReturn(rawAppMeta);
    return row;
  }

  private static Object createAppScanEntry(String appName, String rawAppMeta) throws Exception {
    StructuredRow row = createMockRow("default", appName, "-SNAPSHOT", rawAppMeta);
    Class<?> entryClass = Class.forName(
        "io.cdap.cdap.internal.app.store.AppMetadataStore$AppScanEntry");
    Constructor<?> ctor = entryClass.getDeclaredConstructor(
        StructuredRow.class, StructuredTable.class, StructuredTable.class, boolean.class);
    ctor.setAccessible(true);
    return ctor.newInstance(row, null, null, false);
  }

  @SuppressWarnings("unchecked")
  private static Optional<ArtifactId> invokeGetArtifactId(Object entry) throws Exception {
    Method method = entry.getClass().getDeclaredMethod("getArtifactId");
    method.setAccessible(true);
    try {
      return (Optional<ArtifactId>) method.invoke(entry);
    } catch (InvocationTargetException e) {
      if (e.getTargetException() instanceof RuntimeException) {
        throw (RuntimeException) e.getTargetException();
      }
      if (e.getTargetException() instanceof Error) {
        throw (Error) e.getTargetException();
      }
      throw new RuntimeException(e.getTargetException());
    }
  }
}
