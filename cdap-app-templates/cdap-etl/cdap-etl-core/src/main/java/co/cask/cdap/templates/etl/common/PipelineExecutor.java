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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.SinkWriter;

import java.util.List;

/**
 * Execution of a source and list of transforms.
 */
public final class PipelineExecutor {
  private final BatchSource source;
  private final List<Transform> transformList;
  private final BatchSink sink;
  private DefaultEmitter previousEmitter;
  private DefaultEmitter currentEmitter;

  public PipelineExecutor(BatchSource source, List<Transform> transforms, BatchSink sink) {
    this.source = source;
    this.transformList = transforms;
    this.sink = sink;
    this.previousEmitter = new DefaultEmitter();
    this.currentEmitter = new DefaultEmitter();
  }

  public void runOneIteration(Object key, Object val, SinkWriter sinkWriter) throws Exception {
    previousEmitter.reset();
    source.emit(key, val, previousEmitter);
    for (Transform transform : transformList) {
      for (Object transformedVal : previousEmitter) {
        transform.transform(transformedVal, currentEmitter);
      }
      previousEmitter.reset();
      DefaultEmitter temp = previousEmitter;
      previousEmitter = currentEmitter;
      currentEmitter = temp;
    }
    for (Object transformedVal : previousEmitter) {
      sink.write(transformedVal, sinkWriter);
    }
  }
}
