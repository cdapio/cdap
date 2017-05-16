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

package co.cask.cdap.internal.app.runtime.schedule.constraint;

import co.cask.cdap.proto.ProtoConstraint;
import co.cask.cdap.proto.ProtoConstraintCodec;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Serialization and deserialization of Triggers as Json.
 */
public class ConstraintCodec extends ProtoConstraintCodec {

  private static final Map<ProtoConstraint.Type, Class<? extends ProtoConstraint>> TYPE_TO_CONSTRAINT =
    ImmutableMap.<ProtoConstraint.Type, Class<? extends ProtoConstraint>>builder()
      .put(ProtoConstraint.Type.CONCURRENCY, ConcurrencyConstraint.class)
      .put(ProtoConstraint.Type.DELAY, DelayConstraint.class)
      .put(ProtoConstraint.Type.LAST_RUN, DurationSinceLastRunConstraint.class)
      .put(ProtoConstraint.Type.TIME_RANGE, TimeRangeConstraint.class)
      .build();

  public ConstraintCodec() {
    super(TYPE_TO_CONSTRAINT);
  }
}
