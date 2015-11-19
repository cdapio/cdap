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

package co.cask.cdap.api.artifact;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the parsing of artifact version.
 */
public class ArtifactVersionTest {

  @Test
  public void testParseVersion() {
    // Versions without suffix
    ArtifactVersion version = new ArtifactVersion("1");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());

    version = new ArtifactVersion("1.2");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());

    version = new ArtifactVersion("1.2.3");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals(Integer.valueOf(3), version.getFix());

    // Versions with suffix
    version = new ArtifactVersion("1-suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals("suffix", version.getSuffix());

    version = new ArtifactVersion("1.2-suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals("suffix", version.getSuffix());

    version = new ArtifactVersion("1.2.3-suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals(Integer.valueOf(3), version.getFix());
    Assert.assertEquals("suffix", version.getSuffix());

    // Versions with different suffix style (. instead of -)
    version = new ArtifactVersion("1.suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals("suffix", version.getSuffix());

    version = new ArtifactVersion("1.2.suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals("suffix", version.getSuffix());

    version = new ArtifactVersion("1.2.3.suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals(Integer.valueOf(3), version.getFix());
    Assert.assertEquals("suffix", version.getSuffix());
  }

  @Test
  public void testParseSuffix() {
    Assert.assertEquals(new ArtifactVersion("1"), new ArtifactVersion("cdap-api-1", true));
    Assert.assertEquals(new ArtifactVersion("1.2"), new ArtifactVersion("cdap-api-1.2", true));
    Assert.assertEquals(new ArtifactVersion("1.2.3"), new ArtifactVersion("cdap-api-1.2.3", true));
    Assert.assertEquals(new ArtifactVersion("1.2.3-snapshot"), new ArtifactVersion("cdap-api-1.2.3-snapshot", true));
  }

  @Test
  public void testCompareVersion() {
    // Equality without suffix
    Assert.assertEquals(0, new ArtifactVersion("1").compareTo(new ArtifactVersion("1")));
    Assert.assertEquals(0, new ArtifactVersion("1.2").compareTo(new ArtifactVersion("1.2")));
    Assert.assertEquals(0, new ArtifactVersion("1.2.3").compareTo(new ArtifactVersion("1.2.3")));

    // Equality with suffix
    Assert.assertEquals(0, new ArtifactVersion("1.suffix").compareTo(new ArtifactVersion("1.suffix")));
    Assert.assertEquals(0, new ArtifactVersion("1.2.suffix").compareTo(new ArtifactVersion("1.2.suffix")));
    Assert.assertEquals(0, new ArtifactVersion("1.2.3.suffix").compareTo(new ArtifactVersion("1.2.3.suffix")));

    // Comparison
    Assert.assertEquals(-1, new ArtifactVersion("2").compareTo(new ArtifactVersion("10")));
    Assert.assertEquals(-1, new ArtifactVersion("1.3").compareTo(new ArtifactVersion("2.1")));
    Assert.assertEquals(-1, new ArtifactVersion("2.1").compareTo(new ArtifactVersion("2.3")));
    Assert.assertEquals(-1, new ArtifactVersion("2.3.1").compareTo(new ArtifactVersion("2.3.2")));

    // Comparision with suffix
    Assert.assertEquals(-1, new ArtifactVersion("2-a").compareTo(new ArtifactVersion("10-a")));
    Assert.assertEquals(-1, new ArtifactVersion("1.3-b").compareTo(new ArtifactVersion("2.1-a")));
    Assert.assertEquals(-1, new ArtifactVersion("2.1-c").compareTo(new ArtifactVersion("2.3-a")));
    Assert.assertEquals(-1, new ArtifactVersion("2.3.1-d").compareTo(new ArtifactVersion("2.3.2-a")));
    Assert.assertEquals(-1, new ArtifactVersion("2.3.2-a").compareTo(new ArtifactVersion("2.3.2-b")));

    // Comparison with missing parts
    Assert.assertEquals(-1, new ArtifactVersion("2").compareTo(new ArtifactVersion("2.3")));
    Assert.assertEquals(-1, new ArtifactVersion("2.3").compareTo(new ArtifactVersion("2.3.1")));

    // Snapshot version is smaller
    Assert.assertEquals(-1, new ArtifactVersion("2.3.1-snapshot").compareTo(new ArtifactVersion("2.3.1")));
  }


  @Test
  public void testValidVersions() {
    // since a version's suffix can not have numbers in it, simply the number 1 is the version
    String versionString = "cdap-etl-batch-3.5-SNAPSHOT1";
    ArtifactVersion artifactVersion = new ArtifactVersion(versionString, true);
    assertVersionEquals("3.5-SNAPSHOT1", 3, 5, null, "SNAPSHOT1", artifactVersion);

    versionString = "s3_analytics-1.0-SNAPSHOT";
    artifactVersion = new ArtifactVersion(versionString, true);
    assertVersionEquals("1.0-SNAPSHOT", 1, 0, null, "SNAPSHOT", artifactVersion);

    // the version regex matches everything starting with the '3.5'. Everything after that is assumed to be the
    // version suffix
    versionString = "cdap-etl-3.5-batch-1.0-SNAPSHOT";
    artifactVersion = new ArtifactVersion(versionString, true);
    assertVersionEquals("3.5-batch-1.0-SNAPSHOT", 3, 5, null, "batch-1.0-SNAPSHOT", artifactVersion);

    versionString = "cdap-etl-batch-3.2.0-suffix";
    artifactVersion = new ArtifactVersion(versionString, true);
    assertVersionEquals("3.2.0-suffix", 3, 2, 0, "suffix", artifactVersion);

    // same as previous version, but without suffix
    versionString = "cdap-etl-batch-3.2.0";
    artifactVersion = new ArtifactVersion(versionString, true);
    assertVersionEquals("3.2.0", 3, 2, 0, null, artifactVersion);

    versionString = "1.0-SNAPSHOT";
    artifactVersion = new ArtifactVersion(versionString, false);
    assertVersionEquals("1.0-SNAPSHOT", 1, 0, null, "SNAPSHOT", artifactVersion);
  }

  @Test
  public void testInvalidVersions() {
    // invalid simply because there are no numbers in the string
    assertInvalidVersion(new ArtifactVersion("cdap-etl-batch-SNAPSHOT", true));

    // invalid because we pass 'false' as second parameter to constructor, indicating to match the entire string
    assertInvalidVersion(new ArtifactVersion("cdap-etl-batch-1.0-SNAPSHOT", false));

    assertInvalidVersion(new ArtifactVersion("xyz"));
    assertInvalidVersion(new ArtifactVersion("xyz-v1.2.3"));
  }

  @Test
  public void testNumberInArtifactName() {
    ArtifactVersion actual = new ArtifactVersion("test1-1.0-SNAPSHOT", true);
    ArtifactVersion expected = new ArtifactVersion("1.0-SNAPSHOT");
    Assert.assertEquals(expected, actual);

    actual = new ArtifactVersion("1-test-1.0-SNAPSHOT", true);
    expected = new ArtifactVersion("1.0-SNAPSHOT");
    Assert.assertEquals(expected, actual);

    actual = new ArtifactVersion("1-test123-1.0-SNAPSHOT", true);
    expected = new ArtifactVersion("1.0-SNAPSHOT");
    Assert.assertEquals(expected, actual);

    actual = new ArtifactVersion("1-test123-1.0.0-1234567890", true);
    expected = new ArtifactVersion("1.0.0-1234567890");
    Assert.assertEquals(expected, actual);

    actual = new ArtifactVersion("1-test123-1.0.0-5.4.3-1234567890", true);
    expected = new ArtifactVersion("1.0.0-5.4.3-1234567890");
    Assert.assertEquals(expected, actual);
  }

  private void assertInvalidVersion(ArtifactVersion artifactVersion) {
    assertVersionEquals(null, null, null, null, null, artifactVersion);
  }

  private void assertVersionEquals(String version, Integer major, Integer minor, Integer fix, String suffix,
                                   ArtifactVersion artifactVersion) {
    Assert.assertEquals(version, artifactVersion.getVersion());
    Assert.assertEquals(major, artifactVersion.getMajor());
    Assert.assertEquals(minor, artifactVersion.getMinor());
    Assert.assertEquals(fix, artifactVersion.getFix());
    Assert.assertEquals(suffix, artifactVersion.getSuffix());
  }
}
