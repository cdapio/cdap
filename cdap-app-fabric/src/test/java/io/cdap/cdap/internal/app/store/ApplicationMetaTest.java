/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import java.time.Instant;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ApplicationMeta}.
 */
public class ApplicationMetaTest {

  @Test
  public void testToStringEqualsAndHashCode() {
    ArtifactId artifactId = new ArtifactId("my-art", new ArtifactVersion("1.0.0"), ArtifactScope.USER);
    ApplicationSpecification spec = new DefaultApplicationSpecification(
        "myApp", "1.0.0", "6.0.0", "app desc", null, artifactId,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    ChangeDetail change = new ChangeDetail("initial deploy", null, "alice", 12345L, true);
    SourceControlMeta scm = new SourceControlMeta("fileHash123", "commit456", Instant.ofEpochMilli(999L));

    ApplicationMeta meta1 = new ApplicationMeta("myApp", spec, change, scm);
    ApplicationMeta meta2 = new ApplicationMeta("myApp", spec, change, scm);
    ApplicationMeta metaNoScm = new ApplicationMeta("myApp", spec, change);

    Assert.assertEquals(meta1, meta2);
    Assert.assertEquals(meta1.hashCode(), meta2.hashCode());
    Assert.assertNotEquals(meta1, metaNoScm);
    Assert.assertNull(metaNoScm.getSourceControlMeta());

    String rendered = meta1.toString();
    Assert.assertTrue(rendered.startsWith("ApplicationMeta{"));
    Assert.assertTrue(rendered.contains("id=myApp"));
    Assert.assertTrue(rendered.contains("spec="));
    Assert.assertTrue(rendered.contains("my-art"));
    Assert.assertTrue(rendered.contains("change=" + change));
    Assert.assertTrue(rendered.contains("sourceControlMeta=" + scm));
  }
}
