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
package io.cdap.cdap.internal.app.runtime.k8s;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Fields;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

public class UserServiceProgramMainTest {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .create();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  public static final class NamespaceStore {
    public static final StructuredTableId NAMESPACES = new StructuredTableId("namespaces");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String NAMESPACE_METADATA_FIELD = "namespace_metadata";

    public static final StructuredTableSpecification NAMESPACE_TABLE_SPEC =
      new StructuredTableSpecification.Builder()
        .withId(NAMESPACES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
                    Fields.stringType(NAMESPACE_METADATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD)
        .build();
  }

  @Test
  public void testSomething() throws IOException {
    List<StructuredTableSpecification> specs = Arrays.asList(NamespaceStore.NAMESPACE_TABLE_SPEC);
    File createdFile = folder.newFile("table.spec.json");
    saveJsonFile(specs, createdFile);

    List<StructuredTableSpecification> restoredSpecs = readJsonFileToList(
      createdFile,
      new TypeToken<List<StructuredTableSpecification>>() { }.getType());

    for (StructuredTableSpecification spec : restoredSpecs) {
      System.out.println(spec.toString());
    }
  }

  private <T> File saveJsonFile(List<T> obj, File file) throws IOException {
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      GSON.toJson(obj, new TypeToken<List<T>>() { }.getType(), writer);
    }
    return file;
  }

  private static <T> List<T> readJsonFileToList(File file, Type type) {
    try (Reader reader = new BufferedReader(new FileReader(file))) {
      return GSON.fromJson(reader, type);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to read %s file at %s",
                      StructuredTableSpecification.class.getTypeName(), file.getAbsolutePath()), e);
    }
  }
}
