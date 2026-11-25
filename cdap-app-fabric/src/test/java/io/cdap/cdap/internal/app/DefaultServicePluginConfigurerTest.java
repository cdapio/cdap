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

package io.cdap.cdap.internal.app;

import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
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
 * Unit tests for {@link DefaultServicePluginConfigurer}.
 */
public class DefaultServicePluginConfigurerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testUsePluginWithMacroEvaluatorHandlesExceptions() throws Exception {
    ArtifactId appArtifact = NamespaceId.DEFAULT.artifact("svc-art", "1.0.0");
    PluginInstantiator instantiator = Mockito.mock(PluginInstantiator.class);
    PluginFinder finder = Mockito.mock(PluginFinder.class);

    io.cdap.cdap.api.artifact.ArtifactId apiArtifactId =
        new io.cdap.cdap.api.artifact.ArtifactId(
            "plugin-art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ArtifactDescriptor descriptor = new ArtifactDescriptor(
        NamespaceId.DEFAULT.getNamespace(),
        apiArtifactId,
        Locations.toLocation(TEMP_FOLDER.newFile()));
    PluginClass pluginClass = PluginClass.builder()
        .setName("svcPlugin")
        .setType("service")
        .setDescription("desc")
        .setClassName(String.class.getName())
        .setProperties(Collections.emptyMap())
        .build();
    Map.Entry<ArtifactDescriptor, PluginClass> entry =
        new AbstractMap.SimpleEntry<>(descriptor, pluginClass);

    Mockito.when(finder.findPlugin(
            Mockito.eq(NamespaceId.DEFAULT), Mockito.eq(appArtifact),
            Mockito.eq("service"), Mockito.eq("svcPlugin"), Mockito.any()))
        .thenReturn(entry);
    Mockito.when(finder.findPlugin(
            Mockito.eq(NamespaceId.DEFAULT), Mockito.eq(appArtifact),
            Mockito.eq("service"), Mockito.eq("missing"), Mockito.any()))
        .thenThrow(new PluginNotExistsException(NamespaceId.DEFAULT, "service", "missing"));

    DefaultServicePluginConfigurer configurer = new DefaultServicePluginConfigurer(
        appArtifact, NamespaceId.DEFAULT, instantiator, finder, getClass().getClassLoader());

    MacroEvaluator evaluator = Mockito.mock(MacroEvaluator.class);
    MacroParserOptions options = MacroParserOptions.DEFAULT;

    Mockito.when(instantiator.newInstance(
            Mockito.any(Plugin.class), Mockito.eq(evaluator), Mockito.eq(options)))
        .thenReturn("evaluatedInstance");
    String value = configurer.usePlugin(
        "service", "svcPlugin", "p1", PluginProperties.builder().build(),
        new PluginSelector(), evaluator, options);
    Assert.assertEquals("evaluatedInstance", value);

    Assert.assertNull(configurer.usePlugin(
        "service", "missing", "pMissing", PluginProperties.builder().build(),
        new PluginSelector(), evaluator, options));

    Mockito.when(instantiator.newInstance(
            Mockito.any(Plugin.class), Mockito.eq(evaluator), Mockito.eq(options)))
        .thenThrow(new IOException("io error"));
    Assert.assertNull(configurer.usePlugin(
        "service", "svcPlugin", "pIo", PluginProperties.builder().build(),
        new PluginSelector(), evaluator, options));

    ClassNotFoundException cnfe = new ClassNotFoundException("missing.SvcPlugin");
    Mockito.reset(instantiator);
    Mockito.when(instantiator.newInstance(
            Mockito.any(Plugin.class), Mockito.eq(evaluator), Mockito.eq(options)))
        .thenThrow(cnfe);
    RuntimeException thrown = Assert.assertThrows(RuntimeException.class,
        () -> configurer.usePlugin(
            "service", "svcPlugin", "pCnfe", PluginProperties.builder().build(),
            new PluginSelector(), evaluator, options));
    Assert.assertSame(cnfe, thrown.getCause());
  }
}
