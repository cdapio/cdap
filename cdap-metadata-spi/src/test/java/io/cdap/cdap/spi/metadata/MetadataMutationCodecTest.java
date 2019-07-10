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

package io.cdap.cdap.spi.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.proto.codec.NamespacedEntityIdCodec;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class MetadataMutationCodecTest {

  @Test
  public void testMetadataMutationCodec() {
    List<MetadataMutation> mutations =
      ImmutableList.of(
        new MetadataMutation.Create(NamespaceId.DEFAULT.toMetadataEntity(),
                                    new Metadata(MetadataScope.SYSTEM, ImmutableSet.of("tag1", "val1")),
                                    Collections.emptyMap()),
        new MetadataMutation.Drop(NamespaceId.DEFAULT.toMetadataEntity()),
        new MetadataMutation.Remove(NamespaceId.DEFAULT.toMetadataEntity()),
        new MetadataMutation.Update(NamespaceId.DEFAULT.toMetadataEntity(), new Metadata(MetadataScope.SYSTEM,
                                                                                         ImmutableSet.of("newtag"))));

    Gson gson = new GsonBuilder()
      .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
      .registerTypeAdapter(Metadata.class, new MetadataCodec())
      .registerTypeAdapter(MetadataMutation.class, new MetadataMutationCodec())
      .create();

    String json = gson.toJson(mutations);
    Assert.assertEquals(mutations, gson.fromJson(json, new TypeToken<List<MetadataMutation>>() { }.getType()));
  }
}
