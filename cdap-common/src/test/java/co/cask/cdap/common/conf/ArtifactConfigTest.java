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

package co.cask.cdap.common.conf;

import co.cask.cdap.common.InvalidArtifactException;
import co.cask.cdap.proto.Id;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

/**
 */
public class ArtifactConfigTest {
  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test(expected = InvalidArtifactException.class)
  public void testBadJsonSyntax() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.jar");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("I am invalid.");
    }

    ArtifactConfig.read(Id.Artifact.from(Id.Namespace.SYSTEM, "r1", "1.0.0"), configFile, null);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testMissingPluginFields() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.jar");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("{ \"plugins\": [ { \"name\": \"something\" } ] }");
    }

    ArtifactConfig.read(Id.Artifact.from(Id.Namespace.SYSTEM, "r1", "1.0.0"), configFile, null);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testMalformedParentArtifact() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.jar");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("{ \"parents\": [ \"r2:[1.0.0,2.0.0) \" ] }");
    }

    ArtifactConfig.read(Id.Artifact.from(Id.Namespace.SYSTEM, "r1", "1.0.0"), configFile, null);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testBadParentArtifactName() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.jar");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("{ \"parents\": [ \"r!2[1.0.0,2.0.0) \" ] }");
    }

    ArtifactConfig.read(Id.Artifact.from(Id.Namespace.SYSTEM, "r1", "1.0.0"), configFile, null);
  }

  @Test(expected = InvalidArtifactException.class)
  public void testBadParentArtifactRange() throws IOException, InvalidArtifactException {
    File configFile = new File(tmpFolder.newFolder(), "r1-1.0.0.jar");
    try (BufferedWriter writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      writer.write("{ \"parents\": [ \"r2(2.0.0,1.0.0) \" ] }");
    }

    ArtifactConfig.read(Id.Artifact.from(Id.Namespace.SYSTEM, "r1", "1.0.0"), configFile, null);
  }
}
