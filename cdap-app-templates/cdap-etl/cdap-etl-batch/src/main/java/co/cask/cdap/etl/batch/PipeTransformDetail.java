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
import co.cask.cdap.etl.api.Destroyable;
import co.cask.cdap.etl.api.Transformation;
import co.cask.cdap.etl.batch.mapreduce.PipeEmitter;
import co.cask.cdap.etl.common.Destroyables;
import com.google.common.base.Throwables;


/**
 * Pipe transform detail which wraps stageName, transformation, emitters for output stages
 */
public class PipeTransformDetail implements Destroyable {
  private final String stageName;
  private final Transformation transformation;
  private final PipeEmitter<PipeTransformDetail> emitter;
  private final boolean removeStageName;
  private final boolean isErrorConsumer;

  public PipeTransformDetail(String stageName, boolean removeStageName, boolean isErrorConsumer,
                             Transformation transformation, PipeEmitter<PipeTransformDetail> emitter) {
    this.stageName = stageName;
    this.removeStageName = removeStageName;
    this.transformation = transformation;
    this.emitter = emitter;
    this.isErrorConsumer = isErrorConsumer;
  }

  public void process(KeyValue<String, Object> value) {
    try {
      if (removeStageName) {
        transformation.transform(value.getValue(), emitter);
      } else {
        transformation.transform(value, emitter);
      }
    } catch (StageFailureException e) {
      // Another stage has already failed, just throw the exception as-is
      throw e;
    } catch (Exception e) {
      Throwable rootCause = Throwables.getRootCause(e);
      // Create StageFailureException to save the Stage information
      throw new StageFailureException(
        String.format("Failed to execute pipeline stage '%s' with the error: %s. Please review your pipeline " +
                        "configuration and check the system logs for more details.", stageName, rootCause.getMessage()),
        rootCause);
    }
  }

  public void addTransformation(String stageName, PipeTransformDetail pipeTransformDetail) {
    emitter.addTransformDetail(stageName, pipeTransformDetail);
  }

  public boolean isErrorConsumer() {
    return isErrorConsumer;
  }

  @Override
  public void destroy() {
    if (transformation instanceof Destroyable) {
      Destroyables.destroyQuietly((Destroyable) transformation);
    }
  }
}
