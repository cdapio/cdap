/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data.stream.decoder;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventDecoder;
import org.apache.hadoop.io.LongWritable;

/**
 * A {@link StreamEventDecoder} that decodes a {@link StreamEvent} into {@link LongWritable} as key and
 * {@link StreamEvent} as value. The key carries the event timestamp, while the value contains the
 * entire {@link StreamEvent}.
 */
public final class IdentityStreamEventDecoder implements StreamEventDecoder<LongWritable, StreamEvent> {

  private final LongWritable key = new LongWritable();

  @Override
  public DecodeResult<LongWritable, StreamEvent> decode(StreamEvent event,
                                                        DecodeResult<LongWritable, StreamEvent> result) {
    key.set(event.getTimestamp());
    return result.setKey(key).setValue(event);
  }
}
