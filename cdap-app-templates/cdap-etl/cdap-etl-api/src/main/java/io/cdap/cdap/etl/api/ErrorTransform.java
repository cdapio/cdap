/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

/**
 * A special type of {@link Transform} that will get as input all errors emitted by the previous stage.
 * A record is emitted as an error using {@link Emitter#emitError(InvalidEntry)}.
 *
 * @param <IN> the type of error record
 * @param <OUT> the type of output record
 */
public abstract class ErrorTransform<IN, OUT> extends Transform<ErrorRecord<IN>, OUT> {
  public static final String PLUGIN_TYPE = "errortransform";
}
