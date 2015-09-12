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

package co.cask.cdap.internal.app.plugins;

import co.cask.cdap.templates.PluginVersion;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for testing the {@link PluginVersion} class.
 */
public class PluginVersionTest {

  @Test
  public void testParseVersion() {
    // Versions without suffix
    PluginVersion version = new PluginVersion("1");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());

    version = new PluginVersion("1.2");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());

    version = new PluginVersion("1.2.3");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals(Integer.valueOf(3), version.getFix());

    // Versions with suffix
    version = new PluginVersion("1-suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals("suffix", version.getSuffix());

    version = new PluginVersion("1.2-suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals("suffix", version.getSuffix());

    version = new PluginVersion("1.2.3-suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals(Integer.valueOf(3), version.getFix());
    Assert.assertEquals("suffix", version.getSuffix());

    // Versions with different suffix style (. instead of -)
    version = new PluginVersion("1.suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals("suffix", version.getSuffix());

    version = new PluginVersion("1.2.suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals("suffix", version.getSuffix());

    version = new PluginVersion("1.2.3.suffix");
    Assert.assertEquals(Integer.valueOf(1), version.getMajor());
    Assert.assertEquals(Integer.valueOf(2), version.getMinor());
    Assert.assertEquals(Integer.valueOf(3), version.getFix());
    Assert.assertEquals("suffix", version.getSuffix());
  }

  @Test
  public void testParseSuffix() {
    Assert.assertEquals(new PluginVersion("1"), new PluginVersion("cdap-api-1", true));
    Assert.assertEquals(new PluginVersion("1.2"), new PluginVersion("cdap-api-1.2", true));
    Assert.assertEquals(new PluginVersion("1.2.3"), new PluginVersion("cdap-api-1.2.3", true));
    Assert.assertEquals(new PluginVersion("1.2.3-snapshot"), new PluginVersion("cdap-api-1.2.3-snapshot", true));
  }

  @Test
  public void testNotParsable() {
    Assert.assertNull(new PluginVersion("xyz").getVersion());
    Assert.assertNull(new PluginVersion("xyz-v1.2.3").getVersion());
  }

  @Test
  public void testCompareVersion() {
    // Equality without suffix
    Assert.assertEquals(0, new PluginVersion("1").compareTo(new PluginVersion("1")));
    Assert.assertEquals(0, new PluginVersion("1.2").compareTo(new PluginVersion("1.2")));
    Assert.assertEquals(0, new PluginVersion("1.2.3").compareTo(new PluginVersion("1.2.3")));

    // Equality with suffix
    Assert.assertEquals(0, new PluginVersion("1.suffix").compareTo(new PluginVersion("1.suffix")));
    Assert.assertEquals(0, new PluginVersion("1.2.suffix").compareTo(new PluginVersion("1.2.suffix")));
    Assert.assertEquals(0, new PluginVersion("1.2.3.suffix").compareTo(new PluginVersion("1.2.3.suffix")));

    // Comparison
    Assert.assertEquals(-1, new PluginVersion("2").compareTo(new PluginVersion("10")));
    Assert.assertEquals(-1, new PluginVersion("1.3").compareTo(new PluginVersion("2.1")));
    Assert.assertEquals(-1, new PluginVersion("2.1").compareTo(new PluginVersion("2.3")));
    Assert.assertEquals(-1, new PluginVersion("2.3.1").compareTo(new PluginVersion("2.3.2")));

    // Comparision with suffix
    Assert.assertEquals(-1, new PluginVersion("2-a").compareTo(new PluginVersion("10-a")));
    Assert.assertEquals(-1, new PluginVersion("1.3-b").compareTo(new PluginVersion("2.1-a")));
    Assert.assertEquals(-1, new PluginVersion("2.1-c").compareTo(new PluginVersion("2.3-a")));
    Assert.assertEquals(-1, new PluginVersion("2.3.1-d").compareTo(new PluginVersion("2.3.2-a")));
    Assert.assertEquals(-1, new PluginVersion("2.3.2-a").compareTo(new PluginVersion("2.3.2-b")));

    // Comparison with missing parts
    Assert.assertEquals(-1, new PluginVersion("2").compareTo(new PluginVersion("2.3")));
    Assert.assertEquals(-1, new PluginVersion("2.3").compareTo(new PluginVersion("2.3.1")));

    // Snapshot version is smaller
    Assert.assertEquals(-1, new PluginVersion("2.3.1-snapshot").compareTo(new PluginVersion("2.3.1")));
  }
}
