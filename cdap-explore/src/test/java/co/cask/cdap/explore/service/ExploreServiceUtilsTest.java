/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.explore.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;

/**
 *
 */
public class ExploreServiceUtilsTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testHiveVersion() throws Exception {
    // This would throw an exception if it didn't pass
    ExploreServiceUtils.checkHiveSupport(getClass().getClassLoader());
  }

  @Test
  public void hijackHiveConfFileTest() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("foo", "bar");
    Assert.assertEquals(1, conf.size());

    File confFile = tmpFolder.newFile("hive-site.xml");
    FileOutputStream fos = new FileOutputStream(confFile);
    try {
      conf.writeXml(fos);
    } finally {
      fos.close();
    }

    File newConfFile = ExploreServiceUtils.hijackHiveConfFile(confFile);

    conf = new Configuration(false);
    conf.addResource(newConfFile.toURI().toURL());

    // If Hadoop config methods in ConfigUtil.java loaded the deprecated configurations, then
    // mapreduce.user.classpath.first, which is the old hadoop configuration, will not be persisted
    if (Configuration.isDeprecated("mapreduce.user.classpath.first")) {
      Assert.assertEquals(2, conf.size());
    } else {
      Assert.assertEquals(3, conf.size());
      Assert.assertEquals("true", conf.get("mapreduce.user.classpath.first"));
    }
    Assert.assertEquals("true", conf.get(Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST));
    Assert.assertEquals("bar", conf.get("foo"));

    // Ensure conf files that are not hive-site.xml are unchanged
    confFile = tmpFolder.newFile("yarn-site.xml");
    Assert.assertEquals(confFile, ExploreServiceUtils.hijackHiveConfFile(confFile));
  }

}
