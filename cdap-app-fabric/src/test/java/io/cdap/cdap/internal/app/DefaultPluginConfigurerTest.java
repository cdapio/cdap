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
 * Unit tests for {@link DefaultPluginConfigurer}.
 */
public class DefaultPluginConfigurerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private Map.Entry<ArtifactDescriptor, PluginClass> createPluginEntry() throws IOException {
    io.cdap.cdap.api.artifact.ArtifactId apiArtifactId =
        new io.cdap.cdap.api.artifact.ArtifactId(
            "plugin-art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ArtifactDescriptor descriptor = new ArtifactDescriptor(
        NamespaceId.DEFAULT.getNamespace(),
        apiArtifactId,
        Locations.toLocation(TEMP_FOLDER.newFile()));
    PluginClass pluginClass = PluginClass.builder()
        .setName("myPlugin")
        .setType("transform")
        .setDescription("desc")
        .setClassName(String.class.getName())
        .setProperties(Collections.emptyMap())
        .build();
    return new AbstractMap.SimpleEntry<>(descriptor, pluginClass);
  }

  @Test
  public void testUsePluginSuccessNotFoundIoAndClassNotFound() throws Exception {
    ArtifactId appArtifact = NamespaceId.DEFAULT.artifact("app-art", "1.0.0");
    PluginInstantiator instantiator = Mockito.mock(PluginInstantiator.class);
    PluginFinder finder = Mockito.mock(PluginFinder.class);
    Map.Entry<ArtifactDescriptor, PluginClass> entry = createPluginEntry();

    Mockito.when(finder.findPlugin(
            Mockito.eq(NamespaceId.DEFAULT), Mockito.eq(appArtifact),
            Mockito.eq("transform"), Mockito.eq("myPlugin"), Mockito.any()))
        .thenReturn(entry);
    Mockito.when(finder.findPlugin(
            Mockito.eq(NamespaceId.DEFAULT), Mockito.eq(appArtifact),
            Mockito.eq("transform"), Mockito.eq("missing"), Mockito.any()))
        .thenThrow(new PluginNotExistsException(NamespaceId.DEFAULT, "transform", "missing"));

    DefaultPluginConfigurer configurer = new DefaultPluginConfigurer(
        appArtifact, NamespaceId.DEFAULT, instantiator, finder);

    Mockito.when(instantiator.newInstance(Mockito.any(Plugin.class))).thenReturn("pluginValue");
    String result = configurer.usePlugin(
        "transform", "myPlugin", "id1", PluginProperties.builder().build(), new PluginSelector());
    Assert.assertEquals("pluginValue", result);

    Assert.assertNull(configurer.usePlugin(
        "transform", "missing", "idMissing", PluginProperties.builder().build(),
        new PluginSelector()));

    Mockito.when(instantiator.newInstance(Mockito.any(Plugin.class)))
        .thenThrow(new IOException("bad jar"));
    Assert.assertNull(configurer.usePlugin(
        "transform", "myPlugin", "idIo", PluginProperties.builder().build(), new PluginSelector()));

    ClassNotFoundException cnfe = new ClassNotFoundException("missing.Plugin");
    Mockito.reset(instantiator);
    Mockito.when(instantiator.newInstance(Mockito.any(Plugin.class))).thenThrow(cnfe);
    RuntimeException thrown = Assert.assertThrows(RuntimeException.class,
        () -> configurer.usePlugin("transform", "myPlugin", "idCnfe",
            PluginProperties.builder().build(), new PluginSelector()));
    Assert.assertSame(cnfe, thrown.getCause());
  }

  @Test
  public void testUsePluginClassSuccessIoAndClassNotFound() throws Exception {
    ArtifactId appArtifact = NamespaceId.DEFAULT.artifact("app-art", "1.0.0");
    PluginInstantiator instantiator = Mockito.mock(PluginInstantiator.class);
    PluginFinder finder = Mockito.mock(PluginFinder.class);
    Map.Entry<ArtifactDescriptor, PluginClass> entry = createPluginEntry();

    Mockito.when(finder.findPlugin(
            Mockito.eq(NamespaceId.DEFAULT), Mockito.eq(appArtifact),
            Mockito.eq("transform"), Mockito.eq("myPlugin"), Mockito.any()))
        .thenReturn(entry);

    DefaultPluginConfigurer configurer = new DefaultPluginConfigurer(
        appArtifact, NamespaceId.DEFAULT, instantiator, finder);

    Mockito.doReturn(String.class).when(instantiator).loadClass(Mockito.any(Plugin.class));
    Class<String> loaded = configurer.usePluginClass(
        "transform", "myPlugin", "cls1", PluginProperties.builder().build(), new PluginSelector());
    Assert.assertEquals(String.class, loaded);

    Mockito.when(instantiator.loadClass(Mockito.any(Plugin.class)))
        .thenThrow(new IOException("io failure"));
    Assert.assertNull(configurer.usePluginClass(
        "transform", "myPlugin", "clsIo", PluginProperties.builder().build(),
        new PluginSelector()));

    ClassNotFoundException cnfe = new ClassNotFoundException("missing.PluginClass");
    Mockito.reset(instantiator);
    Mockito.when(instantiator.loadClass(Mockito.any(Plugin.class))).thenThrow(cnfe);
    RuntimeException thrown = Assert.assertThrows(RuntimeException.class,
        () -> configurer.usePluginClass("transform", "myPlugin", "clsCnfe",
            PluginProperties.builder().build(), new PluginSelector()));
    Assert.assertSame(cnfe, thrown.getCause());
  }
}
