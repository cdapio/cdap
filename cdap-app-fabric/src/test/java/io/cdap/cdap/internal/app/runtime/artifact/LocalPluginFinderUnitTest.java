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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link LocalPluginFinder}.
 */
public class LocalPluginFinderUnitTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testFindPluginDelegatesAndPropagatesExceptions() throws Exception {
    ArtifactRepository repo = Mockito.mock(ArtifactRepository.class);
    LocalPluginFinder finder = new LocalPluginFinder(repo);

    ArtifactId parentId = NamespaceId.DEFAULT.artifact("parent-art", "2.1.0");
    ArtifactRange expectedRange = new ArtifactRange(
        NamespaceId.DEFAULT.getNamespace(), "parent-art",
        new ArtifactVersion("2.1.0"), true, new ArtifactVersion("2.1.0"), true);
    PluginSelector selector = new PluginSelector();

    ArtifactDescriptor descriptor = new ArtifactDescriptor(
        NamespaceId.DEFAULT.getNamespace(),
        new io.cdap.cdap.api.artifact.ArtifactId(
            "plugin-art", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
        Locations.toLocation(TEMP_FOLDER.newFile()));
    PluginClass pluginClass = PluginClass.builder()
        .setName("p1")
        .setType("source")
        .setDescription("desc")
        .setClassName("com.example.Source")
        .setProperties(Collections.emptyMap())
        .build();
    Map.Entry<ArtifactDescriptor, PluginClass> expectedEntry =
        new AbstractMap.SimpleEntry<>(descriptor, pluginClass);

    Mockito.when(repo.findPlugin(
            NamespaceId.DEFAULT, expectedRange, "source", "p1", selector))
        .thenReturn(expectedEntry);
    Assert.assertSame(expectedEntry,
        finder.findPlugin(NamespaceId.DEFAULT, parentId, "source", "p1", selector));

    // PluginNotExistsException is rethrown directly.
    PluginNotExistsException notExists =
        new PluginNotExistsException(NamespaceId.DEFAULT, "source", "missing");
    Mockito.when(repo.findPlugin(
            NamespaceId.DEFAULT, expectedRange, "source", "missing", selector))
        .thenThrow(notExists);
    try {
      finder.findPlugin(NamespaceId.DEFAULT, parentId, "source", "missing", selector);
      Assert.fail("Expected PluginNotExistsException");
    } catch (PluginNotExistsException actual) {
      Assert.assertSame(notExists, actual);
    }

    // Checked IOException and ArtifactNotFoundException are wrapped in RuntimeException.
    IOException ioException = new IOException("store read error");
    Mockito.when(repo.findPlugin(
            NamespaceId.DEFAULT, expectedRange, "source", "ioFail", selector))
        .thenThrow(ioException);
    RuntimeException wrappedIo = Assert.assertThrows(RuntimeException.class,
        () -> finder.findPlugin(NamespaceId.DEFAULT, parentId, "source", "ioFail", selector));
    Assert.assertSame(ioException, wrappedIo.getCause());

    ArtifactNotFoundException artifactNotFound = new ArtifactNotFoundException(parentId);
    Mockito.when(repo.findPlugin(
            NamespaceId.DEFAULT, expectedRange, "source", "missingParent", selector))
        .thenThrow(artifactNotFound);
    RuntimeException wrappedNotFound = Assert.assertThrows(RuntimeException.class,
        () -> finder.findPlugin(
            NamespaceId.DEFAULT, parentId, "source", "missingParent", selector));
    Assert.assertSame(artifactNotFound, wrappedNotFound.getCause());

    // Unchecked RuntimeException propagates unwrapped.
    IllegalStateException expectedRuntime = new IllegalStateException("unexpected runtime");
    Mockito.when(repo.findPlugin(
            NamespaceId.DEFAULT, expectedRange, "source", "rtFail", selector))
        .thenThrow(expectedRuntime);
    try {
      finder.findPlugin(NamespaceId.DEFAULT, parentId, "source", "rtFail", selector);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }
  }
}
