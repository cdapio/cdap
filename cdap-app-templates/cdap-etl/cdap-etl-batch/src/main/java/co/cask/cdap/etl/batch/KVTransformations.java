/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.common.Constants;

/**
 * Key-Value transformation which wraps transformation for each batch plugin to emit stageName along with each record
 * except for {@link BatchSink}
 */
public final class KVTransformations {

  private KVTransformations() {
  }

  /**
   * Creates {@link Transformation} which adds stageName to each record being emitted from current stage,
   * except for {@link BatchSink}. Each stage will strip off stageName, apply original transformation and then again
   * wrap record with stageName. For {@link co.cask.cdap.etl.api.Joiner} the record is passed with stageName.
   *
   * @param stageName      stageName which is emitting the record
   * @param pluginType     type of the stage
   * @param isMapPhase     if it is map phase
   * @param transformation transformation to be wrapped
   * @return {@link Transformation} to wrap/unwrap stageName
   */
  public static Transformation getKVTransformation(String stageName, String pluginType, boolean isMapPhase,
                                                   Transformation transformation) {
    if (BatchSink.PLUGIN_TYPE.equalsIgnoreCase(pluginType)) {
      return new KVSinkTransformation<>(transformation);
    } else if (BatchSource.PLUGIN_TYPE.equalsIgnoreCase(pluginType)) {
      return new KVSourceTransformation<>(stageName, transformation);
    } else if (Constants.CONNECTOR_TYPE.equalsIgnoreCase(pluginType)) {
      return transformation;
    } else if (BatchJoiner.PLUGIN_TYPE.equalsIgnoreCase(pluginType)) {
      if (isMapPhase) {
        return transformation;
      } else {
        return new KVSourceTransformation<>(stageName, transformation);
      }
    } else if (BatchAggregator.PLUGIN_TYPE.equalsIgnoreCase(pluginType)) {
      if (isMapPhase) {
        return new KVSinkTransformation<>(transformation);
      } else {
        return new KVSourceTransformation<>(stageName, transformation);
      }
    }
    return new KVWrappedTransformation(stageName, transformation);
  }

  /**
   * Converts input to (stageName, input)
   *
   * @param <IN>  type of input
   * @param <OUT> type of output
   */
  public static class KVSourceTransformation<IN, OUT> implements Transformation<IN, KeyValue<String, OUT>> {
    private final String stageName;
    private final Transformation<IN, OUT> transformation;

    public KVSourceTransformation(String stageName, Transformation<IN, OUT> transformation) {
      this.stageName = stageName;
      this.transformation = transformation;
    }

    @Override
    public void transform(IN input, final Emitter<KeyValue<String, OUT>> emitter) throws Exception {
      transformation.transform(input, new Emitter<OUT>() {
        @Override
        public void emit(OUT value) {
          emitter.emit(new KeyValue<>(stageName, value));
        }
        @Override
        public void emitError(InvalidEntry<OUT> error) {
          emitter.emitError(new InvalidEntry<>(error.getErrorCode(), error.getErrorMsg(),
                                               new KeyValue<>(stageName, error.getInvalidRecord())));
        }
      });
    }
  }

  /**
   * Converts (stageName, input) to input
   *
   * @param <IN>  type of input
   * @param <OUT> type of output
   */
  public static class KVSinkTransformation<IN, OUT> implements Transformation<KeyValue<String, IN>, OUT> {
    private final Transformation<IN, OUT> transformation;

    public KVSinkTransformation(Transformation<IN, OUT> transformation) {
      this.transformation = transformation;
    }

    @Override
    public void transform(KeyValue<String, IN> input, Emitter<OUT> emitter) throws Exception {
      transformation.transform(input.getValue(), emitter);
    }
  }

  /**
   * Unwraps (stageName, input) to input, applies transformation and wraps output to (stageName, output)
   *
   * @param <IN>  type of input
   * @param <OUT> type of output
   */
  public static class KVWrappedTransformation<IN, OUT> implements Transformation<KeyValue<String, IN>, KeyValue<String,
    OUT>> {
    private final String stageName;
    private final Transformation<IN, OUT> transformation;

    public KVWrappedTransformation(String stageName, Transformation<IN, OUT> transformation) {
      this.stageName = stageName;
      this.transformation = transformation;
    }

    @Override
    public void transform(KeyValue<String, IN> input, final Emitter<KeyValue<String, OUT>> emitter) throws Exception {
      transformation.transform(input.getValue(), new Emitter<OUT>() {
        @Override
        public void emit(OUT value) {
          emitter.emit(new KeyValue<>(stageName, value));
        }
        @Override
        public void emitError(InvalidEntry<OUT> error) {
          emitter.emitError(new InvalidEntry<>(error.getErrorCode(), error.getErrorMsg(),
                                               new KeyValue<>(stageName, error.getInvalidRecord())));
        }
      });
    }
  }
}
