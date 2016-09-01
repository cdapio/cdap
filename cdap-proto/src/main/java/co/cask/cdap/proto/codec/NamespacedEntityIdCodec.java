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

package co.cask.cdap.proto.codec;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.NamespacedEntityId;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;

import java.lang.reflect.Type;

/**
 * Codec for {@link co.cask.cdap.proto.id.NamespacedEntityId}. Currently only supports
 * {@link co.cask.cdap.proto.id.ApplicationId}, {@link co.cask.cdap.proto.id.ArtifactId},
 * {@link co.cask.cdap.proto.id.ProgramId}, {@link co.cask.cdap.proto.id.DatasetId},
 * {@link co.cask.cdap.proto.id.StreamId} and {@link co.cask.cdap.proto.id.StreamViewId}.
 *
 * Currently just delegates to {@link NamespacedEntityIdCodec} while phasing out {@link Id} usages.
 */
public class NamespacedEntityIdCodec extends AbstractSpecificationCodec<NamespacedEntityId> {

  private NamespacedIdCodec namespacedIdCodec;

  public NamespacedEntityIdCodec() {
    this.namespacedIdCodec = new NamespacedIdCodec();
  }

  @Override
  public JsonElement serialize(NamespacedEntityId src, Type typeOfSrc, JsonSerializationContext context) {
    return namespacedIdCodec.serialize((Id.NamespacedId) src.toId(), typeOfSrc, context);
  }

  @Override
  public NamespacedEntityId deserialize(JsonElement json, Type typeOfT,
                                     JsonDeserializationContext context) throws JsonParseException {
    return (NamespacedEntityId) namespacedIdCodec.deserialize(json, typeOfT, context).toEntityId();
  }

}
