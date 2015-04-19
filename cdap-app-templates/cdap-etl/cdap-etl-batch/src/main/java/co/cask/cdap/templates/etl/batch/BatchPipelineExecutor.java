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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkWriter;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.common.DefaultEmitter;
import co.cask.cdap.templates.etl.common.TransformExecutor;

import java.util.List;

/**
 * Execution of a source and list of transforms.
 */
public final class BatchPipelineExecutor extends TransformExecutor {
  private final BatchSource source;
  private final BatchSink sink;
  private final DefaultEmitter defaultEmitter;

  public BatchPipelineExecutor(BatchSource source, List<Transform> transforms, BatchSink sink) {
    super(transforms);
    this.source = source;
    this.sink = sink;
    this.defaultEmitter = new DefaultEmitter();
  }

  public void runOneIteration(Object key, Object val, BatchSinkWriter batchSinkWriter) throws Exception {
    defaultEmitter.reset();
    source.emit(key, val, defaultEmitter);
    for (Object sourceObj : defaultEmitter) {
      Iterable<Object> sinkObjects = runOneIteration(sourceObj);
      for (Object sinkObj : sinkObjects) {
        sink.write(sinkObj, batchSinkWriter);
      }
    }
  }
}
