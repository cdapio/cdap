/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.program;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.archive.ArchiveBundler;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.InputSupplier;
import org.apache.twill.filesystem.Location;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.Manifest;

/**
 *
 */
public final class ProgramBundle {
  private static final String APPLICATION_META_ENTRY = "application.json";
  private static final Predicate<JarEntry> META_IGNORE = new Predicate<JarEntry>() {
    @Override
    public boolean apply(JarEntry input) {
      return input.getName().contains("MANIFEST.MF") || input.getName().contains(APPLICATION_META_ENTRY);
    }
  };

  /**
   * Clones a give application archive using the {@link co.cask.cdap.archive.ArchiveBundler}.
   * A new manifest file will be amended to the jar.
   *
   * @return An instance of {@link Location} containing the program JAR.
   *
   * @throws java.io.IOException in case of any issue related to copying jars.
   */
  public static Location create(Id.Application id, ArchiveBundler bundler, Location output, String programName,
                                String className, ProgramType type,
                                ApplicationSpecification appSpec) throws IOException {
    return create(id, bundler, output, programName, className, type, appSpec, null);
  }

  public static Location create(Id.Application id, ArchiveBundler bundler, Location output, String programName,
                               String className, ProgramType type, ApplicationSpecification appSpec,
                               Manifest other) throws IOException {
    // Create a MANIFEST file
    Manifest manifest = new Manifest();

    // Copy over attributes from other manifest
    if (other != null) {
      for (Map.Entry<Object, Object> entry : other.getMainAttributes().entrySet()) {
        Attributes.Name key = (Attributes.Name) entry.getKey();
        String value = (String) entry.getValue();
        manifest.getMainAttributes().put(key, value);
      }
    }

    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, ManifestFields.VERSION);
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, className);
    manifest.getMainAttributes().put(ManifestFields.PROCESSOR_TYPE, type.toString());
    manifest.getMainAttributes().put(ManifestFields.SPEC_FILE, ManifestFields.MANIFEST_SPEC_FILE);
    manifest.getMainAttributes().put(ManifestFields.ACCOUNT_ID, id.getNamespaceId());
    manifest.getMainAttributes().put(ManifestFields.APPLICATION_ID, id.getId());
    manifest.getMainAttributes().put(ManifestFields.PROGRAM_NAME, programName);

    bundler.clone(output, manifest, ImmutableMap.of(APPLICATION_META_ENTRY, getInputSupplier(appSpec)), META_IGNORE);
    return output;
  }

  private static InputSupplier<InputStream> getInputSupplier(final ApplicationSpecification appSpec) {
    return new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        String json = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(appSpec);
        return new ByteArrayInputStream(json.getBytes(Charsets.UTF_8));
      }
    };
  }
}
