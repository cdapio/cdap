/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.util.hbase;


import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

/**
 *
 */
public class HBaseVersionTest {
  @Test
  public void testHBaseVersions() throws ParseException {
    // test CDH HBase SNAPSHOT version
    String version = "1.2.0-cdh5.7.1-SNAPSHOT";
    HBaseVersion.VersionNumber versionNumber = HBaseVersion.VersionNumber.create(version);
    Assert.assertEquals(new Integer(1), versionNumber.getMajor());
    Assert.assertEquals(new Integer(2), versionNumber.getMinor());
    Assert.assertEquals(new Integer(0), versionNumber.getPatch());
    Assert.assertNull(versionNumber.getLast());
    Assert.assertEquals("cdh5.7.1", versionNumber.getClassifier());
    Assert.assertTrue(versionNumber.isSnapshot());

    // test HDP HBase version
    version = "1.1.2.2.5.3.0-37";
    versionNumber = HBaseVersion.VersionNumber.create(version);
    Assert.assertEquals(new Integer(1), versionNumber.getMajor());
    Assert.assertEquals(new Integer(1), versionNumber.getMinor());
    Assert.assertEquals(new Integer(2), versionNumber.getPatch());
    Assert.assertNull(versionNumber.getLast());
    Assert.assertNull(versionNumber.getClassifier());
    Assert.assertFalse(versionNumber.isSnapshot());

    // test IBM HBase version
    version = "1.2.0-IBM-7";
    versionNumber = HBaseVersion.VersionNumber.create(version);
    Assert.assertEquals(new Integer(1), versionNumber.getMajor());
    Assert.assertEquals(new Integer(2), versionNumber.getMinor());
    Assert.assertEquals(new Integer(0), versionNumber.getPatch());
    Assert.assertNull(versionNumber.getLast());
    Assert.assertEquals("IBM", versionNumber.getClassifier());
    Assert.assertFalse(versionNumber.isSnapshot());

    // test HBase version
    version = "1.1.1";
    versionNumber = HBaseVersion.VersionNumber.create(version);
    Assert.assertEquals(new Integer(1), versionNumber.getMajor());
    Assert.assertEquals(new Integer(1), versionNumber.getMinor());
    Assert.assertEquals(new Integer(1), versionNumber.getPatch());
    Assert.assertNull(versionNumber.getLast());
    Assert.assertNull(versionNumber.getClassifier());
    Assert.assertFalse(versionNumber.isSnapshot());
  }

  @Test
  public void testHBaseVersionToCompatMapping() throws ParseException {
    // test the mapping from hbase version to the compat module we use for that version
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_96, "0.96.1.1");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_98, "0.98.1");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_98, "0.98.6");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_10, "1.0.0");

    assertCompatModuleMapping(HBaseVersion.Version.HBASE_10_CDH, "1.0.0-cdh5.4.4");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_10_CDH55, "1.0.0-cdh5.5.2");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_10_CDH56, "1.0.0-cdh5.6.1");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_11, "1.1.1");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_11, "1.2.0-IBM-7");
    assertCompatModuleMapping(HBaseVersion.Version.UNKNOWN, "1.4.0");

    // hbase versions packaged with HDP
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_96, "0.96.1.2.0.13.0-43-hadoop2"); // hdp 2.0.13.0
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_98, "0.98.0.2.1.15.0-946-hadoop2"); // hdp 2.1.15.0
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_11, "1.1.2.2.3.6.0-3796"); // hdp 2.3.6.0
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_11, "1.1.2.2.4.3.0-227"); // hdp 2.4.3.0
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_11, "1.1.2.2.5.3.0-37"); // hdp 2.5.3.0
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_11, "1.1.2.2.6.1.0-129"); // hdp 2.6.1.0

    assertCompatModuleMapping(HBaseVersion.Version.HBASE_12_CDH57, "1.2.0-cdh5.7.1");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_12_CDH57, "1.2.0-cdh5.7.1-SNAPSHOT");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_12_CDH57, "1.2.0-cdh5.8.2");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_12_CDH57, "1.2.0-cdh5.9.0");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_12_CDH57, "1.2.0-cdh5.10.0");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_12_CDH57, "1.2.0-cdh5.11.0");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_12_CDH57, "1.2.0-cdh5.12.0");
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_12_CDH57, "1.2.0-cdh5.13.0");

    // bigtop 1.1.0
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_98, "0.98.12-hadoop2");

    // EMR 5.4.0 - 5.6.0
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_11, "1.3.0");
    // EMR 5.7.0+
    assertCompatModuleMapping(HBaseVersion.Version.HBASE_11, "1.3.1");
    // make sure that if future CDH versions come out with hbase 1.3.0, don't assume they're compatible with HBASE_11
    assertCompatModuleMapping(HBaseVersion.Version.UNKNOWN_CDH, "1.3.0-cdh5.15.0");
    assertCompatModuleMapping(HBaseVersion.Version.UNKNOWN_CDH, "1.3.0-cdh5.16.0");
  }

  private void assertCompatModuleMapping(HBaseVersion.Version expectedCompatModule,
                                         String hbaseVersion) throws ParseException {
    Assert.assertEquals(expectedCompatModule, HBaseVersion.determineVersionFromVersionString(hbaseVersion));
  }
}
