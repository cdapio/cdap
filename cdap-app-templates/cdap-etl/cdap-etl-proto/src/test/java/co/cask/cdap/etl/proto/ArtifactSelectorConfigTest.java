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

package co.cask.cdap.etl.proto;

import co.cask.cdap.api.artifact.ArtifactScope;
import org.junit.Test;

/**
 */
public class ArtifactSelectorConfigTest {

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidName() {
    ArtifactSelectorConfig config = new ArtifactSelectorConfig(ArtifactScope.USER.name(), "abc?d", "1.0.0");
    config.getArtifactSelector(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidVersion() {
    ArtifactSelectorConfig config = new ArtifactSelectorConfig(ArtifactScope.USER.name(), "abc", "abc");
    config.getArtifactSelector(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidScope() {
    ArtifactSelectorConfig config = new ArtifactSelectorConfig("usr", "abc", "1.0.0");
    config.getArtifactSelector(null, null);
  }

}
