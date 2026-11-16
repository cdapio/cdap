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

package io.cdap.cdap.internal.app.runtime;

import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultPluginContext}.
 */
public class DefaultPluginContextTest {

  private static final ProgramId PROGRAM_ID =
      NamespaceId.DEFAULT.app("app").program(ProgramType.SPARK, "spark");

  private Plugin createSamplePlugin() {
    ArtifactId artifactId =
        new ArtifactId("plugin-art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    PluginClass pluginClass = PluginClass.builder()
        .setName("p1")
        .setType("transform")
        .setDescription("desc")
        .setClassName(String.class.getName())
        .setProperties(Collections.emptyMap())
        .build();
    return new Plugin(
        Collections.emptyList(),
        artifactId,
        pluginClass,
        PluginProperties.builder().add("k", "v").build());
  }

  @Test
  public void testLoadPluginClassExceptionHandling() throws Exception {
    Plugin plugin = createSamplePlugin();
    Map<String, Plugin> plugins = Collections.singletonMap("pid", plugin);
    FeatureFlagsProvider flags = new FeatureFlagsProvider() {
      @Override
      public boolean isFeatureEnabled(String name) {
        return "enabledFlag".equals(name);
      }
    };
    PluginInstantiator instantiator = Mockito.mock(PluginInstantiator.class);

    DefaultPluginContext context =
        new DefaultPluginContext(instantiator, PROGRAM_ID, plugins, flags);
    Assert.assertTrue(context.isFeatureEnabled("enabledFlag"));
    Assert.assertFalse(context.isFeatureEnabled("other"));
    Assert.assertEquals("v", context.getPluginProperties("pid").getProperties().get("k"));

    Mockito.doReturn(String.class).when(instantiator).loadClass(plugin);
    Assert.assertEquals(String.class, context.loadPluginClass("pid"));

    IOException ioException = new IOException("corrupt plugin jar");
    Mockito.when(instantiator.loadClass(plugin)).thenThrow(ioException);
    RuntimeException wrappedIo = Assert.assertThrows(
        RuntimeException.class, () -> context.loadPluginClass("pid"));
    Assert.assertSame(ioException, wrappedIo.getCause());

    Mockito.reset(instantiator);
    Mockito.when(instantiator.loadClass(plugin))
        .thenThrow(new ClassNotFoundException("missing"));
    Assert.assertThrows(IllegalArgumentException.class, () -> context.loadPluginClass("pid"));

    Mockito.reset(instantiator);
    IllegalStateException expectedRuntime = new IllegalStateException("unexpected runtime");
    Mockito.when(instantiator.loadClass(plugin)).thenThrow(expectedRuntime);
    try {
      context.loadPluginClass("pid");
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }
  }

  @Test
  public void testNewPluginInstanceExceptionHandling() throws Exception {
    Plugin plugin = createSamplePlugin();
    Map<String, Plugin> plugins = Collections.singletonMap("pid", plugin);
    FeatureFlagsProvider flags = new FeatureFlagsProvider() { };
    PluginInstantiator instantiator = Mockito.mock(PluginInstantiator.class);

    DefaultPluginContext context =
        new DefaultPluginContext(instantiator, PROGRAM_ID, plugins, flags);

    Mockito.when(instantiator.newInstance(plugin, null)).thenReturn("created");
    Assert.assertEquals("created", context.newPluginInstance("pid"));

    IOException ioException = new IOException("cannot unpack plugin jar");
    Mockito.when(instantiator.newInstance(plugin, null)).thenThrow(ioException);
    RuntimeException wrappedIo = Assert.assertThrows(
        RuntimeException.class, () -> context.newPluginInstance("pid"));
    Assert.assertSame(ioException, wrappedIo.getCause());

    Mockito.reset(instantiator);
    InvalidPluginConfigException invalidConfig =
        new InvalidPluginConfigException("invalid k", null);
    Mockito.when(instantiator.newInstance(plugin, null)).thenThrow(invalidConfig);
    try {
      context.newPluginInstance("pid");
      Assert.fail("Expected InvalidPluginConfigException");
    } catch (InvalidPluginConfigException actual) {
      Assert.assertSame(invalidConfig, actual);
    }

    DefaultPluginContext unsupportedContext =
        new DefaultPluginContext(null, PROGRAM_ID, plugins, flags);
    Assert.assertThrows(
        UnsupportedOperationException.class, () -> unsupportedContext.loadPluginClass("pid"));
    Assert.assertThrows(
        UnsupportedOperationException.class, () -> unsupportedContext.newPluginInstance("pid"));
    Assert.assertThrows(
        IllegalArgumentException.class, () -> context.loadPluginClass("unknownId"));
  }
}
