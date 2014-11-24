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
 * Interface for Flowlet to implement in order to receive callback on atomic change result.
 */
public interface Callback {

  /**
   * This method will be called when handling a given change in the Flowlet is completed successfully.
   *
   * @param change The change object that was handled successfully.
   *               In case this method is called after processing input, and if the process method is annotated
   *               with {@link co.cask.cdap.api.annotation.Batch}, the
   *               change object will be of type {@link java.util.Iterator}, while inside the iterator contains
   *               all event objects sent to the process method within a batch.
   *               In case this method is called after changing the number of instances for the Flowlet,
   *               the change object will be the past number of instances.
   * @param atomicContext The {@link AtomicContext} that was given to the handling method.
   */
  void onSuccess(@Nullable Object change, AtomicContext atomicContext);

  /**
   * This method will be called when handling a given change in the Flowlet has failed.
   * Failure could be triggered due to exception thrown in the handling method or a system error.
   * The return value of this method is used to determine what action to take about the failure.
   *
   * @param change The change object that was handled successfully.
   *               In case this method is called after processing input, and if the process method is annotated
   *               with {@link co.cask.cdap.api.annotation.Batch}, the
   *               change object will be of type {@link java.util.Iterator}, while inside the iterator contains
   *               all event objects sent to the process method within a batch.
   *               In case this method is called after changing the number of instances for the Flowlet,
   *               the change object will be the past number of instances.
   * @param atomicContext The {@link AtomicContext} that was given to the handling method.
   * @param reason Reason for the failure.
   * @return A {@link FailurePolicy} indicating how to handle the failure.
   */
  FailurePolicy onFailure(@Nullable Object change, AtomicContext atomicContext, FailureReason reason);

  /**
   * This method will be called when the number of instances for the flowlet is changed, for all the instances
   * of the flowlet which existed before the change. Each method call is transactional.
   * The {@link Callback#onFailure} method will be called in case this method fails. The {@link AtomicContext.Type}
   * will be set to {@code INSTANCE_CHANGE}.
   *
   * @param flowletContext the {@link FlowletContext} of the flowlet.
   * @param previousInstancesCount the number of flowlet instances there was before the change
   * @throws Exception in case of any error.
   */
  void onChangeInstances(FlowletContext flowletContext, int previousInstancesCount) throws Exception;
}
