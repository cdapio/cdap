/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.api.artifact;

import org.junit.Assert;
import org.junit.Test;

public class ArtifactVersionTest {

  @Test
  public void testCompare() {
    // test major version
    ArtifactVersion smaller = new ArtifactVersion("1.5.0");
    ArtifactVersion larger = new ArtifactVersion("2.0.0-SNAPSHOT");
    assertVersion(smaller, larger);

    // test minor version
    smaller = new ArtifactVersion("3.1.8");
    larger = new ArtifactVersion("3.2.0");
    assertVersion(smaller, larger);

    // test fix version
    smaller = new ArtifactVersion("3.2.0");
    larger = new ArtifactVersion("3.2.5-SNAPSHOT");
    assertVersion(smaller, larger);

    // test one is snapshot and other is released but without suffix
    smaller = new ArtifactVersion("6.0.0-SNAPSHOT");
    larger = new ArtifactVersion("6.0.0");
    assertVersion(smaller, larger);

    // test one is snapshot and other is released but with suffix
    smaller = new ArtifactVersion("6.0.0-SNAPSHOT");
    larger = new ArtifactVersion("6.0.0.0");
    assertVersion(smaller, larger);

    // test both are released versions with suffix
    smaller = new ArtifactVersion("6.0.0.0");
    larger = new ArtifactVersion("6.0.0.2");
    assertVersion(smaller, larger);

    // test both are released but one has a suffix
    smaller = new ArtifactVersion("6.0.0");
    larger = new ArtifactVersion("6.0.0.0");
    assertVersion(smaller, larger);

    // test both are snapshots
    smaller = new ArtifactVersion("6.0.0-SNAPSHOT");
    larger = new ArtifactVersion("6.0.0-SNAPSHOT1");
    assertVersion(smaller, larger);

    // test other scenarios
    smaller = new ArtifactVersion("6");
    larger = new ArtifactVersion("6.0");
    assertVersion(smaller, larger);
    smaller = new ArtifactVersion("6.0");
    larger = new ArtifactVersion("6.0.0");
    assertVersion(smaller, larger);

    // test equal artifacts
    smaller = new ArtifactVersion("6.0.0");
    larger = new ArtifactVersion("6.0.0");
    Assert.assertEquals(0, smaller.compareTo(larger));
    smaller = new ArtifactVersion("6.0.0.0");
    larger = new ArtifactVersion("6.0.0.0");
    Assert.assertEquals(0, smaller.compareTo(larger));
    smaller = new ArtifactVersion("6.0.0-SNAPSHOT");
    larger = new ArtifactVersion("6.0.0-SNAPSHOT");
    Assert.assertEquals(0, smaller.compareTo(larger));
  }

  private void assertVersion(ArtifactVersion smaller, ArtifactVersion larger) {
    Assert.assertTrue(smaller.compareTo(larger) < 0);
    Assert.assertTrue(larger.compareTo(smaller) > 0);
  }
}
