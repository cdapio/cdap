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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.api.Transformation;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes Transforms one iteration at a time, tracking how many records were input into and output from
 * each transform.
 *
 * @param <IN> the type of input object to the first transform
 * @param <OUT> the type of object output by the last transform
 */
public class TransformExecutor<IN, OUT> implements Destroyable {


  private static final Logger LOG = LoggerFactory.getLogger(TransformExecutor.class);

  private final List<TransformDetail> transforms;
  private final List<DefaultEmitter> emitters;


  public TransformExecutor(List<TransformDetail> transformDetailList) {
    int numTransforms = transformDetailList.size();
    this.transforms = new ArrayList<>(numTransforms);
    this.emitters = Lists.newArrayListWithCapacity(numTransforms);

    for (TransformDetail transformDetail : transformDetailList) {
      this.transforms.add(new TransformDetail(transformDetail,
                                              new TrackedTransform(transformDetail.getTransformation(),
                                                                   transformDetail.getMetrics())));
      this.emitters.add(new DefaultEmitter(transformDetail.getMetrics()));
    }
  }

  public TransformResponse<OUT> runOneIteration(IN input) throws Exception {

    Map<String, Collection<OUT>> errorRecordsMap = new HashMap<>(transforms.size());

    if (transforms.isEmpty()) {
      return new TransformResponse<>(Lists.newArrayList((OUT) input).iterator(), errorRecordsMap);
    }

    TransformDetail transformDetail = transforms.get(0);
    DefaultEmitter currentEmitter = emitters.get(0);
    currentEmitter.reset();
    Transformation transform = transformDetail.getTransformation();
    transform.transform(input, currentEmitter);

    if (!currentEmitter.getErrors().isEmpty()) {
      errorRecordsMap.put(transformDetail.getTransformId(), currentEmitter.getErrors());
    }

    DefaultEmitter previousEmitter = currentEmitter;
    for (int i = 1; i < transforms.size(); i++) {
      transformDetail = transforms.get(i);
      transform = transformDetail.getTransformation();
      currentEmitter = emitters.get(i);
      for (Object transformedVal : previousEmitter) {
        transform.transform(transformedVal, currentEmitter);
      }
      if (!currentEmitter.getErrors().isEmpty()) {
        errorRecordsMap.put(transformDetail.getTransformId(), currentEmitter.getErrors());
      }
      previousEmitter = currentEmitter;
    }

    return new TransformResponse<>(previousEmitter.iterator(), errorRecordsMap);
  }

  public void resetEmitters() {
    for (DefaultEmitter<OUT> emitter : emitters) {
      emitter.reset();
    }
  }

  @Override
  public void destroy() {
    for (TransformDetail transformDetail : transforms) {
      Transformation transformation = transformDetail.getTransformation();
      if (transformation instanceof Destroyable) {
        Destroyables.destroyQuietly((Destroyable) transformation);
      }
    }
  }
}
