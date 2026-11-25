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

package io.cdap.cdap.data2.dataset2.lib.file;

import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.DefaultNamespacePathLocator;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link FileSetDataset} property resolution and output cleanup.
 */
public class FileSetDatasetTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testFormatAndPermissionFallbackAndOverride() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    LocationFactory lf = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
    NamespacePathLocator pathLocator = new DefaultNamespacePathLocator(cConf, lf, namespaceAdmin);
    pathLocator.get(NamespaceId.DEFAULT).mkdirs();
    DatasetContext context = DatasetContext.from(NamespaceId.DEFAULT.getNamespace());

    // Case 1: Both spec and runtime arguments omit format/permissions -> null without NPE
    DatasetSpecification emptySpec = DatasetSpecification.builder("fsEmpty", "fileSet").build();
    FileSetDataset emptyFs =
        new FileSetDataset(context, cConf, emptySpec, lf, pathLocator, Collections.emptyMap());
    Assert.assertNull(emptyFs.getInputFormatClassName());
    Assert.assertNull(emptyFs.getOutputFormatClassName());

    // Case 2: Spec defines formats and permissions; runtime arguments are empty -> falls back to spec
    DatasetSpecification configuredSpec = DatasetSpecification.builder("fsConfigured", "fileSet")
        .properties(FileSetProperties.builder()
            .setInputFormat(TextInputFormat.class)
            .setOutputFormat(TextOutputFormat.class)
            .setFilePermissions("rwxr-x---")
            .build()
            .getProperties())
        .build();
    Map<String, String> baseRuntimeArgs = new HashMap<>();
    FileSetArguments.setOutputPath(baseRuntimeArgs, "out1");
    FileSetArguments.setInputPaths(baseRuntimeArgs, Collections.singletonList("in1"));

    FileSetDataset fallbackFs =
        new FileSetDataset(context, cConf, configuredSpec, lf, pathLocator, baseRuntimeArgs);
    Assert.assertEquals(TextInputFormat.class.getName(), fallbackFs.getInputFormatClassName());
    Assert.assertEquals(TextOutputFormat.class.getName(), fallbackFs.getOutputFormatClassName());
    Assert.assertTrue(fallbackFs.getInputFormatConfiguration()
        .get(FileInputFormat.INPUT_DIR).endsWith("/in1"));
    Assert.assertEquals("027", fallbackFs.getOutputFormatConfiguration()
        .get("fs.permissions.umask-mode"));

    // Case 3: Runtime arguments override input/output format and permissions
    Map<String, String> overrideArgs = new HashMap<>(FileSetProperties.builder()
        .setInputFormat("custom.RuntimeInputFormat")
        .setOutputFormat("custom.RuntimeOutputFormat")
        .setFilePermissions("rwx------")
        .build()
        .getProperties());
    FileSetArguments.setBaseOutputPath(overrideArgs);

    FileSetDataset overriddenFs =
        new FileSetDataset(context, cConf, configuredSpec, lf, pathLocator, overrideArgs);
    Assert.assertEquals("custom.RuntimeInputFormat", overriddenFs.getInputFormatClassName());
    Assert.assertEquals("custom.RuntimeOutputFormat", overriddenFs.getOutputFormatClassName());
    Map<String, String> outConf = overriddenFs.getOutputFormatConfiguration();
    Assert.assertNotNull(outConf.get(FileOutputFormat.OUTDIR));
    Assert.assertEquals("077", outConf.get("fs.permissions.umask-mode"));
  }

  @Test
  public void testOnFailureDeletesEmptyOutputDirOnly() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    LocationFactory lf = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
    NamespacePathLocator pathLocator = new DefaultNamespacePathLocator(cConf, lf, namespaceAdmin);
    pathLocator.get(NamespaceId.DEFAULT).mkdirs();
    DatasetContext context = DatasetContext.from(NamespaceId.DEFAULT.getNamespace());
    DatasetSpecification spec = DatasetSpecification.builder("fsFailure", "fileSet").build();

    Map<String, String> emptyDirArgs = new HashMap<>();
    FileSetArguments.setOutputPath(emptyDirArgs, "emptyOut");
    FileSetDataset emptyOutFs =
        new FileSetDataset(context, cConf, spec, lf, pathLocator, emptyDirArgs);
    Location emptyOutLoc = emptyOutFs.getOutputLocation();
    Assert.assertTrue(emptyOutLoc.mkdirs());
    Assert.assertTrue(emptyOutLoc.exists());
    emptyOutFs.onFailure();
    Assert.assertFalse(emptyOutLoc.exists());

    Map<String, String> populatedArgs = new HashMap<>();
    FileSetArguments.setOutputPath(populatedArgs, "populatedOut");
    FileSetDataset populatedOutFs =
        new FileSetDataset(context, cConf, spec, lf, pathLocator, populatedArgs);
    Location populatedLoc = populatedOutFs.getOutputLocation();
    Assert.assertTrue(populatedLoc.mkdirs());
    try (OutputStream os = populatedLoc.append("part-0").getOutputStream()) {
      os.write("keep".getBytes(StandardCharsets.UTF_8));
    }
    populatedOutFs.onFailure();
    Assert.assertTrue(populatedLoc.exists());
  }
}
