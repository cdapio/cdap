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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.template.etl.api.Destroyable;
import co.cask.cdap.template.etl.api.Transformation;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes Transforms one iteration at a time, tracking how many records were input into and output from
 * each transform.
 *
 * @param <IN> the type of input object to the first transform
 * @param <OUT> the type of object output by the last transform
 */
public class TransformExecutor<IN, OUT> implements Destroyable {

  private static final Logger LOG = LoggerFactory.getLogger(TransformExecutor.class);

  private final List<TransformationDetails> transforms;
  private final List<DefaultEmitter> emitters;


  public TransformExecutor(List<TransformationDetails> transformationDetailsList) {
    int numTransforms = transformationDetailsList.size();
    this.transforms = new ArrayList<>(numTransforms);
    this.emitters = Lists.newArrayListWithCapacity(numTransforms);

    for (TransformationDetails transformationDetails : transformationDetailsList) {
      this.transforms.add(new TransformationDetails(transformationDetails,
                                                    new TrackedTransform(transformationDetails.getTransformation(),
                                                                         transformationDetails.getMetrics())));
      this.emitters.add(new DefaultEmitter(transformationDetails.getMetrics()));
    }
  }

  public TransformResponse<OUT> runOneIteration(IN input) throws Exception {
    List<TransformResponse.TransformError<OUT>> errorIterators = new ArrayList<>();
    if (transforms.isEmpty()) {
      return new TransformResponse<OUT>(Lists.newArrayList((OUT) input).iterator(), errorIterators);
    }

    TransformationDetails transformationDetails = transforms.get(0);
    DefaultEmitter currentEmitter = emitters.get(0);
    currentEmitter.reset();
    Transformation transform = transformationDetails.getTransformation();
    transform.transform(input, currentEmitter);

    // if there are errors in the iterator, we add it to the errorIterators list
    if (currentEmitter.getErrorIterator().hasNext()) {
      errorIterators.add(new TransformResponse.TransformError<OUT>(transformationDetails.getTransformId(),
                                                                 currentEmitter.getErrorIterator()));
    }

    DefaultEmitter previousEmitter = currentEmitter;
    for (int i = 1; i < transforms.size(); i++) {
      transformationDetails = transforms.get(i);
      transform = transformationDetails.getTransformation();
      currentEmitter = emitters.get(i);
      currentEmitter.reset();
      for (Object transformedVal : previousEmitter) {
        transform.transform(transformedVal, currentEmitter);
      }
      previousEmitter.reset();
      previousEmitter = currentEmitter;

      // if there are errors in the iterator, we add it to the errorIterators list
      if (currentEmitter.getErrorIterator().hasNext()) {
        errorIterators.add(new TransformResponse.TransformError<OUT>(transformationDetails.getTransformId(),
                                                                     currentEmitter.getErrorIterator()));
      }
    }

    return new TransformResponse<OUT>(previousEmitter.iterator(), errorIterators);
  }

  @Override
  public void destroy() {
    for (TransformationDetails transformationDetails : transforms) {
      Transformation transformation = transformationDetails.getTransformation();
      if (transformation instanceof Destroyable) {
        Destroyables.destroyQuietly((Destroyable) transformation);
      }
    }
  }
}
