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

package co.cask.cdap.templates.etl.lib.sources.realtime;

import co.cask.cdap.templates.etl.api.realtime.Emitter;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.api.stages.AbstractRealtimeSource;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/**
 * RealtimeSource Kafka Source
 */
public class KafkaSource extends AbstractRealtimeSource<ByteBuffer> {

  @Override
  public void onSuspend() {

  }

  @Override
  public void onResume(int oldInstance, int newInstance) {

  }

  @Nullable
  @Override
  public SourceState poll(Emitter<ByteBuffer> writer, @Nullable SourceState currentState) {
    return null;
  }
}
