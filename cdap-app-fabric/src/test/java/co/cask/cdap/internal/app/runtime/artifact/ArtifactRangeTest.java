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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.InvalidArtifactRangeException;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ArtifactRangeTest {

  @Test
  public void testIsInRange() {
    ArtifactRange range = new ArtifactRange(Constants.DEFAULT_NAMESPACE_ID, "test",
      new ArtifactVersion("1.0.0"), new ArtifactVersion("2.0.0"));

    Assert.assertFalse(range.versionIsInRange(new ArtifactVersion("0.0.9")));
    Assert.assertFalse(range.versionIsInRange(new ArtifactVersion("0.9.0")));
    Assert.assertFalse(range.versionIsInRange(new ArtifactVersion("0.9")));
    // 1 < 1.0 < 1.0.0-SNAPSHOT < 1.0.0
    Assert.assertFalse(range.versionIsInRange(new ArtifactVersion("1")));
    Assert.assertFalse(range.versionIsInRange(new ArtifactVersion("1.0")));
    Assert.assertFalse(range.versionIsInRange(new ArtifactVersion("1.0.0-SNAPSHOT")));
    Assert.assertTrue(range.versionIsInRange(new ArtifactVersion("1.0.0")));
    Assert.assertTrue(range.versionIsInRange(new ArtifactVersion("1.0.1-SNAPSHOT")));
    Assert.assertTrue(range.versionIsInRange(new ArtifactVersion("1.0.1")));
    Assert.assertTrue(range.versionIsInRange(new ArtifactVersion("1.1.0")));
    // 2 < 2.0 < 2.0.0-SNAPSHOT < 2.0.0
    Assert.assertTrue(range.versionIsInRange(new ArtifactVersion("2")));
    Assert.assertTrue(range.versionIsInRange(new ArtifactVersion("2.0")));
    Assert.assertTrue(range.versionIsInRange(new ArtifactVersion("2.0.0-SNAPSHOT")));
    Assert.assertFalse(range.versionIsInRange(new ArtifactVersion("2.0.0")));
    Assert.assertFalse(range.versionIsInRange(new ArtifactVersion("2.0.1")));
  }

  @Test
  public void testVersionParse() throws InvalidArtifactRangeException {
    ArtifactRange expected = new ArtifactRange(Constants.DEFAULT_NAMESPACE_ID, "test",
      new ArtifactVersion("1.0.0"), true, new ArtifactVersion("2.0.0-SNAPSHOT"), false);
    ArtifactRange actual = ArtifactRange.parse(Constants.DEFAULT_NAMESPACE_ID, "test[1.0.0,2.0.0-SNAPSHOT)");
    Assert.assertEquals(expected, actual);

    expected = new ArtifactRange(Constants.DEFAULT_NAMESPACE_ID, "test",
      new ArtifactVersion("0.1.0-SNAPSHOT"), false, new ArtifactVersion("1.0.0"), true);
    actual = ArtifactRange.parse(Constants.DEFAULT_NAMESPACE_ID, "test(0.1.0-SNAPSHOT,1.0.0]");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testParseInvalid() {
    // test can't find '[' or '('
    try {
      ArtifactRange.parse(Constants.DEFAULT_NAMESPACE_ID, "test-1.0.0,2.0.0]");
      Assert.fail();
    } catch (InvalidArtifactRangeException e) {
      // expected
    }

    // test can't find ',' between versions
    try {
      ArtifactRange.parse(Constants.DEFAULT_NAMESPACE_ID, "test[1.0.0:2.0.0]");
      Assert.fail();
    } catch (InvalidArtifactRangeException e) {
      // expected
    }

    // test no ending ']' or ')'
    try {
      ArtifactRange.parse(Constants.DEFAULT_NAMESPACE_ID, "test[1.0.0,2.0.0");
      Assert.fail();
    } catch (InvalidArtifactRangeException e) {
      // expected
    }

    // test invalid lower version
    try {
      ArtifactRange.parse(Constants.DEFAULT_NAMESPACE_ID, "tes[t1.0.0,2.0.0]");
      Assert.fail();
    } catch (InvalidArtifactRangeException e) {
      // expected
    }

    try {
      ArtifactRange.parse(Constants.DEFAULT_NAMESPACE_ID, "test(,1.0.0)");
      Assert.fail();
    } catch (InvalidArtifactRangeException e) {
      // expected
    }
    try {
      ArtifactRange.parse(Constants.DEFAULT_NAMESPACE_ID, "test(1.0.0,)");
      Assert.fail();
    } catch (InvalidArtifactRangeException e) {
      // expected
    }
  }
}
