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

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.InvalidArtifactRangeException;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class ArtifactRangeTest {

  @Test
  public void testWhitespace() throws InvalidArtifactRangeException {
    ArtifactRange range = ArtifactRange.parse(NamespaceId.DEFAULT, "name[ 1.0.0 , 2.0.0 )");
    Assert.assertEquals(new ArtifactRange(NamespaceId.DEFAULT, "name",
                                          new ArtifactVersion("1.0.0"), true,
                                          new ArtifactVersion("2.0.0"), false),
                        range);
  }

  @Test
  public void testIsInRange() {
    ArtifactRange range = new ArtifactRange(NamespaceId.DEFAULT, "test",
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
    ArtifactRange expected = new ArtifactRange(NamespaceId.DEFAULT, "test",
      new ArtifactVersion("1.0.0"), true, new ArtifactVersion("2.0.0-SNAPSHOT"), false);
    ArtifactRange actual = ArtifactRange.parse(NamespaceId.DEFAULT, "test[1.0.0,2.0.0-SNAPSHOT)");
    Assert.assertEquals(expected, actual);

    expected = new ArtifactRange(NamespaceId.DEFAULT, "test",
      new ArtifactVersion("0.1.0-SNAPSHOT"), false, new ArtifactVersion("1.0.0"), true);
    actual = ArtifactRange.parse(NamespaceId.DEFAULT, "test(0.1.0-SNAPSHOT,1.0.0]");
    Assert.assertEquals(expected, actual);

    // test compatible with toString
    Assert.assertEquals(expected, ArtifactRange.parse(expected.toString()));
  }

  @Test
  public void testLowerVersionGreaterThanUpper() {
    List<String> invalidRanges = Lists.newArrayList(
      "test[1.0.0,0.9.9]",
      "test[1.0.0,1.0.0-SNAPSHOT]",
      "test[1.0.0,1.0.0)",
      "test(1.0.0,0.9.9)",
      "test(1.0.0,1.0.0-SNAPSHOT)",
      "test(1.0.2,1.0.0]"
    );

    for (String invalidRange : invalidRanges) {
      try {
        ArtifactRange.parse(NamespaceId.DEFAULT, invalidRange);
        Assert.fail();
      } catch (InvalidArtifactRangeException e) {
        // expected
      }
      try {
        ArtifactRange.parse("system:" + invalidRange);
        Assert.fail();
      } catch (InvalidArtifactRangeException e) {
        // expected
      }
    }
  }

  @Test
  public void testParseInvalid() {
    List<String> invalidRanges = Lists.newArrayList(
      // can't find '[' or '('
      "test-1.0.0,2.0.0]",
      // can't find ',' between versions
      "test[1.0.0:2.0.0]",
      // no ending ']' or ')'
      "test[1.0.0,2.0.0",
      // invalid lower version
      "tes[t1.0.0,2.0.0]",
      // missing versions
      "test(,1.0.0)",
      "test(1.0.0,)",
      // invalid name
      "te$t[1.0.0,2.0.0)",
      // namespace only is InvalidArtifactRangeException and not an out of bounds exception
      "default:"
    );

    for (String invalidRange : invalidRanges) {
      try {
        ArtifactRange.parse(NamespaceId.DEFAULT, invalidRange);
        Assert.fail();
      } catch (InvalidArtifactRangeException e) {
        // expected
      }
      try {
        ArtifactRange.parse("system:" + invalidRange);
        Assert.fail();
      } catch (InvalidArtifactRangeException e) {
        // expected
      }
    }
  }
}
