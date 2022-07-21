/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.common.conf;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

/**
 */
public class ArtifactConfigReaderTest {
  private static final ArtifactConfigReader configReader = new ArtifactConfigReader();
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testRead() throws IOException, InvalidArtifactException {
    ArtifactConfig validConfig = new ArtifactConfig(
      ImmutableSet.of(
        new ArtifactRange(NamespaceId.SYSTEM.getNamespace(), "a",
                          new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0")),
        new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), "b",
                          new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"))
      ),
      ImmutableSet.of(
        PluginClass.builder().setName("name").setType("type").setDescription("desc")
          .setClassName("classname").setProperties(ImmutableMap.of(
          "x", new PluginPropertyField("x", "some field", "int", true, false),
          "y", new PluginPropertyField("y", "some other field", "string", false, false))).build()),
      ImmutableMap.of(
        "k1", "v1",
        "k2", "v2"
      )
    );
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.json");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write(validConfig.toString());
    }

    Assert.assertEquals(validConfig, configReader.read(Id.Namespace.DEFAULT, configFile));
  }

  @Test(expected = InvalidArtifactException.class)
  public void testInvalidParentNamespace() throws IOException, InvalidArtifactException {
    ArtifactConfig badConfig = new ArtifactConfig(
      ImmutableSet.of(
        new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), "b",
                          new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"))
      ),
      ImmutableSet.<PluginClass>of(),
      ImmutableMap.<String, String>of());
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.json");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write(badConfig.toString());
    }

    configReader.read(Id.Namespace.SYSTEM, configFile);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testBadJsonSyntax() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.json");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("I am invalid.");
    }

    configReader.read(Id.Namespace.SYSTEM, configFile);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testMissingPluginFields() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.json");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("{ \"plugins\": [ { \"name\": \"something\" } ] }");
    }

    configReader.read(Id.Namespace.SYSTEM, configFile);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testMalformedParentArtifact() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.json");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("{ \"parents\": [ \"r2:[1.0.0,2.0.0) \" ] }");
    }

    configReader.read(Id.Namespace.SYSTEM, configFile);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testBadParentArtifactName() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.json");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("{ \"parents\": [ \"r!2[1.0.0,2.0.0) \" ] }");
    }

    configReader.read(Id.Namespace.SYSTEM, configFile);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testBadParentArtifactRange() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.json");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("{ \"parents\": [ \"r2(2.0.0,1.0.0) \" ] }");
    }

    configReader.read(Id.Namespace.SYSTEM, configFile);
  }
}
