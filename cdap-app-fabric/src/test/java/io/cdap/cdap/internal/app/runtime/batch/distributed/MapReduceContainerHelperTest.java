/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch.distributed;

import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link MapReduceContainerHelper}.
 */
public class MapReduceContainerHelperTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testAddMapReduceClassPathWithAndWithoutFrameworkPath() {
    Configuration hConf = new Configuration();
    hConf.set(YarnConfiguration.YARN_APPLICATION_CLASSPATH, " /yarn/lib1 , /yarn/lib2 ");
    String mrPath = " /mr/a , , /mr/b" + File.pathSeparatorChar + "/mr/c ";
    hConf.set(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH, mrPath);

    List<String> withoutFramework =
        MapReduceContainerHelper.addMapReduceClassPath(hConf, new ArrayList<>());
    Assert.assertEquals(
        Arrays.asList("/yarn/lib1", "/yarn/lib2", "/mr/a", "/mr/b", "/mr/c"),
        withoutFramework);

    hConf.set(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, "file:///opt/mr-framework.tar.gz");
    List<String> withFramework =
        MapReduceContainerHelper.addMapReduceClassPath(hConf, new ArrayList<>());
    Assert.assertEquals(Arrays.asList("/mr/a", "/mr/b", "/mr/c"), withFramework);
  }

  @Test
  public void testLocalizeFrameworkHandlesUnsetInvalidMissingAndValidPaths() throws Exception {
    Configuration unsetConf = new Configuration();
    Map<String, LocalizeResource> emptyMap =
        MapReduceContainerHelper.localizeFramework(unsetConf, new HashMap<>());
    Assert.assertTrue(emptyMap.isEmpty());

    Configuration invalidUriConf = new Configuration();
    invalidUriConf.set(MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, "http://[::1:bad");
    Assert.assertTrue(
        MapReduceContainerHelper.localizeFramework(invalidUriConf, new HashMap<>()).isEmpty());

    Configuration missingFileConf = new Configuration();
    File missingArchive = new File(tempFolder.getRoot(), "missing-framework.tar.gz");
    missingFileConf.set(
        MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH, missingArchive.toURI().toString());
    Assert.assertTrue(
        MapReduceContainerHelper.localizeFramework(missingFileConf, new HashMap<>()).isEmpty());

    File archiveFile = tempFolder.newFile("mr-framework.tar.gz");
    Files.write(archiveFile.toPath(), "archive-bytes".getBytes(StandardCharsets.UTF_8));
    Configuration validConf = new Configuration();
    validConf.set(
        MRJobConfig.MAPREDUCE_APPLICATION_FRAMEWORK_PATH,
        archiveFile.toURI().toString() + "#mr-framework-alias");

    Map<String, LocalizeResource> localized =
        MapReduceContainerHelper.localizeFramework(validConf, new HashMap<>());
    Assert.assertEquals(1, localized.size());
    LocalizeResource resource = localized.get("mr-framework-alias");
    Assert.assertNotNull(resource);
    Assert.assertTrue(resource.isArchive());
    Assert.assertEquals(archiveFile.getAbsolutePath(), resource.getURI().getPath());
    Assert.assertNull(resource.getURI().getFragment());
  }
}
