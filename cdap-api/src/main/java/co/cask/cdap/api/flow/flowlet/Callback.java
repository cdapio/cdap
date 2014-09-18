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

package co.cask.cdap.api.flow.flowlet;

import javax.annotation.Nullable;

/**
 * Interface for flowlet to implement in order to receive callback on process result.
 */
public interface Callback {

  /**
   * This method will be called when processing of a given input is completed successfully.
   *
   * @param input The input object that was given to the process method.
   *              If the process method is annotated with {@link co.cask.cdap.api.annotation.Batch}, the
   *              input object will be of type {@link java.util.Iterator}, while inside the iterator contains
   *              all event objects sent to the process method within a batch.
   * @param inputContext The {@link InputContext} that was given to the process method.
   */
  void onSuccess(@Nullable Object input, @Nullable InputContext inputContext);

  /**
   * This method will be called when processing of a given input failed. Failure could be triggered due to
   * exception thrown in the process method or a system error. The return value of this method
   * is used to determine what action to take about the failure input.
   *
   * @param input The input object that was given to the process method.
   *              If the process method is annotated with {@link co.cask.cdap.api.annotation.Batch}, the
   *              input object will be of type {@link java.util.Iterator}, while inside the iterator contains
   *              all event objects sent to the process method within a batch.
   * @param inputContext The {@link InputContext} that was given to the process method.
   * @param reason Reason for the failure.
   * @return A {@link FailurePolicy} indicating how to handle the failure input.
   */
  FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason);
}
