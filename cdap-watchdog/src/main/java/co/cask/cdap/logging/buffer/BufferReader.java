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

package co.cask.cdap.logging.buffer;

import co.cask.cdap.logging.pipeline.buffer.BufferLogProcessorPipeline;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Buffer reader to read log events from buffer and pass them to log processing pipeline
 * {@link BufferLogProcessorPipeline}.
 */
public interface BufferReader {
  /**
   * Reads log events from provided file offset. If the offset it null, starts reading first file from start position.
   * TODO return an iterator instead of list.
   * @param offset
   * @param batchSize
   * @return
   */
  List<BufferLogEvent> read(@Nullable FileOffset offset, int batchSize);
}
