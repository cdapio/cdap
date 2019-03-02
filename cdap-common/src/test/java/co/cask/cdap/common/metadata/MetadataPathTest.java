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

package co.cask.cdap.common.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class MetadataPathTest {

  @Test
  public void testMetadataEntityFromPath() {
    Map<String, MetadataEntity> testCases = ImmutableMap.<String, MetadataEntity>builder()
      .put("/v3/namespaces/default/suffix",
           NamespaceId.DEFAULT.toMetadataEntity())

      .put("/v3/namespaces/nn/apps/pipe/versions/-SNAPSHOT/suffix",
           new NamespaceId("nn").app("pipe").toMetadataEntity())

      .put("/v3/namespaces/nn/apps/pipe/versions/1.0.1/suffix",
           new NamespaceId("nn").app("pipe", "1.0.1").toMetadataEntity())

      .put("/v3/namespaces/nn/apps/pipe/suffix",
           new NamespaceId("nn").app("pipe").toMetadataEntity())

      .put("/v3/namespaces/nn/apps/pipe/mapreduce/pp/suffix",
           new NamespaceId("nn").app("pipe").program(ProgramType.MAPREDUCE, "pp").toMetadataEntity())

      .put("/v3/namespaces/nn/apps/pipe/services/pp/suffix",
           new NamespaceId("nn").app("pipe").program(ProgramType.SERVICE, "pp").toMetadataEntity())

      .put("/v3/namespaces/nn/apps/pipe/versions/42/services/pp/suffix",
           new NamespaceId("nn").app("pipe", "42").program(ProgramType.SERVICE, "pp").toMetadataEntity())

      .put("/v3/namespaces/nn/apps/pipe/versions/-SNAPSHOT/workers/pp/suffix",
           new NamespaceId("nn").app("pipe").program(ProgramType.WORKER, "pp").toMetadataEntity())

      .put("/v3/namespaces/nn/apps/pipe/versions/42/workflows/pp/suffix",
           new NamespaceId("nn").app("pipe", "42").program(ProgramType.WORKFLOW, "pp").toMetadataEntity())

      .put("/v3/namespaces/nn/artifacts/art/versions/1.0/suffix",
           MetadataEntity.builder(MetadataEntity.ofNamespace("nn")).appendAsType(MetadataEntity.ARTIFACT, "art")
             .append(MetadataEntity.VERSION, "1.0").build())

      .put("/v3/namespaces/nn/dataset/dd/suffix",
           MetadataEntity.ofDataset("nn", "dd"))

      .put("/v3/namespaces/nn/dataset/dd/field/ff/suffix",
           MetadataEntity.builder(MetadataEntity.ofDataset("nn", "dd")).appendAsType("field", "ff").build())

      .put("/v3/one/1/two/2/suffix",
           MetadataEntity.builder().append("one", "1").append("two", "2").build())

      .put("/v3/one/1/services/2/suffix",
           MetadataEntity.builder().append("one", "1").append("services", "2").build())

      .put("/v3/dataset/pipe/field/ff/suffix",
           MetadataEntity.builder().append("dataset", "pipe").append("field", "ff").build())

      .build();

    for (Map.Entry<String, MetadataEntity> test : testCases.entrySet()) {
      Assert.assertEquals("For path '" + test.getKey() + "': ",
                          test.getValue(), MetadataPath.getMetadataEntityFromPath(test.getKey(),
                                                                                  "/v3/", "/suffix", null));
    }
  }

  @Test
  public void testInvalidMetadataEntityInPath() {
    String[] tests = {
      "/v3/namespaces/nn/versions/1.0.1/APPLICATION/pipe/suffix", "APPLICATION",
      "/v3/namespaces/nn/versions/1.0.1/apps/pipe/suffix", "app",
      "/v3/namespaces/nn/versions/1.0.1/apps/suffix", "apps",
      "/v3/PROGRAM/pipe/suffix", null,
      "/v3/program/pipe/suffix", null,
      "/v3/program/pipe/suffix", "program",
      "/v3/program/pipe/suffix", "service",
      "/v3/dataset/pipe/field/ff/suffix", "dataset",
      "/v3/dataset/pipe/field/ff/suffix", "fields",
      "", null
    };

    for (int i = 0; i + 1 < tests.length; i += 2) {
      try {
        MetadataEntity entity = MetadataPath.getMetadataEntityFromPath(tests[i], "/v3/", "/suffix", tests[i + 1]);
        Assert.fail("Expected IllegalArgumentException for '" + tests[i] + "' with type '" + tests[i + 1] + "' " +
                      "but got " + entity);
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

}
