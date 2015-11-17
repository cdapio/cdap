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
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.Transformation;
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

  private final Map<String, List<TransformInformation>> transformInfos;
  private final String start;


  class TransformInformation {
    TransformDetail transformDetail;
    DefaultEmitter emitter;
    private boolean destroyed;

    TransformInformation(TransformDetail transformDetail, DefaultEmitter emitter) {
      this.transformDetail = transformDetail;
      this.emitter = emitter;
      this.destroyed = false;
    }

    TransformDetail getTransformDetail() {
      return transformDetail;
    }

    DefaultEmitter getEmitter() {
      return emitter;
    }

    void reset() {
      emitter.reset();
    }

    void destroy() {
      if (!destroyed) {
        Transformation transformation = transformDetail.getTransformation();
        if (transformation instanceof Destroyable) {
          Destroyables.destroyQuietly((Destroyable) transformation);
        }
        destroyed = true;
      }
    }
  }

  public TransformExecutor(Map<String, List<TransformDetail>> transformDetailList, String start) {
    this.transformInfos = new HashMap<>();
    this.start = start;
    Map<String, TransformInformation> transformInformationMap = new HashMap<>();

    for (Map.Entry<String, List<TransformDetail>> transformEntry : transformDetailList.entrySet()) {
      transformInfos.put(transformEntry.getKey(), new ArrayList<TransformInformation>());

      for (TransformDetail transformDetail : transformEntry.getValue()) {
        if (!transformInformationMap.containsKey(transformDetail.getTransformId())) {
          transformInformationMap.put(
            transformDetail.getTransformId(),
            new TransformInformation(
              new TransformDetail(transformDetail,
                                  new TrackedTransform(transformDetail.getTransformation(),
                                                       transformDetail.getMetrics())),
              new DefaultEmitter(transformDetail.getMetrics())
            ));

        }

        transformInfos.get(transformEntry.getKey()).add(transformInformationMap.get(transformDetail.getTransformId()));
      }
    }
  }

  public TransformResponse runOneIteration(IN input) throws Exception {
    if (transformInfos.isEmpty()) {
      // Note: this should not happen
      throw new IllegalArgumentException("No Connections found to either transform or sink..");
    }

    // just add the input to an emitter..
    DefaultEmitter sourceEmitter = new DefaultEmitter<>(new StageMetrics() {
      @Override
      public void count(String metricName, int delta) {

      }

      @Override
      public void gauge(String metricName, long value) {

      }

      @Override
      public void pipelineCount(String metricName, int delta) {

      }

      @Override
      public void pipelineGauge(String metricName, long value) {

      }
    });
    sourceEmitter.emit(input);

    Map<String, DefaultEmitter> sinksOutput = new HashMap<>();
    Map<String, Collection> transformErrors = new HashMap<>();
    transformGraph(sinksOutput, sourceEmitter, transformErrors, start);
    return new TransformResponse(sinksOutput, transformErrors);
  }

  private void transformGraph(Map<String, DefaultEmitter> sinksOutput,
                      DefaultEmitter previousEmitter, Map<String, Collection> transformErrors,
                      String stageName) throws Exception {

    for (TransformInformation transformInformation : transformInfos.get(stageName)) {
      TransformDetail transformDetail = transformInformation.getTransformDetail();
      if (!transformDetail.isSink()) {
        transformInformation.getEmitter().reset();
        for (Object transformedVal : previousEmitter) {
          transformDetail.getTransformation().transform(transformedVal, transformInformation.getEmitter());
        }
        if (!transformInformation.getEmitter().getErrors().isEmpty()) {
          transformErrors.put(transformDetail.getTransformId(), transformInformation.getEmitter().getErrors());
        }
        transformGraph(sinksOutput, transformInformation.getEmitter(),
                       transformErrors, transformDetail.getTransformId());
      } else {
        // if its a sink, add the previous emitter entries to the sink output list
        if (!sinksOutput.containsKey(transformDetail.getTransformId())) {
          sinksOutput.put(transformDetail.getTransformId(), transformInformation.getEmitter());
        }
        DefaultEmitter emitter = sinksOutput.get(transformDetail.getTransformId());
        for (Object previousValue : previousEmitter) {
          emitter.emit(previousValue);
        }
      }
    }
  }

  public void resetEmitters() {
    for (List<TransformInformation> transformInformationList : transformInfos.values()) {
      for (TransformInformation transformInformation : transformInformationList) {
        transformInformation.reset();
      }
    }
  }

  @Override
  public void destroy() {
    for (List<TransformInformation> transformInformationList : transformInfos.values()) {
      for (TransformInformation transformInformation : transformInformationList) {
        transformInformation.destroy();
      }
    }
  }
}
