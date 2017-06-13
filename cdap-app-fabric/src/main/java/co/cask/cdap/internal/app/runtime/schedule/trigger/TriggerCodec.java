/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.trigger;

import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.ProtoTriggerCodec;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Serialization and deserialization of Triggers as Json.
 */
public class TriggerCodec extends ProtoTriggerCodec {

  private static final Map<ProtoTrigger.Type, Class<? extends Trigger>> TYPE_TO_INTERNAL_TRIGGER =
    ImmutableMap.<ProtoTrigger.Type, Class<? extends Trigger>>builder()
      .put(ProtoTrigger.Type.TIME, TimeTrigger.class)
      .put(ProtoTrigger.Type.PARTITION, PartitionTrigger.class)
      .put(ProtoTrigger.Type.STREAM_SIZE, StreamSizeTrigger.class)
      .put(ProtoTrigger.Type.PROGRAM_STATUS, ProgramStatusTrigger.class)
      .build();

  public TriggerCodec() {
    super(TYPE_TO_INTERNAL_TRIGGER);
  }
}
