package com.continuuity.api.flow.flowlet;

import javax.annotation.Nullable;

/**
 * Interface for flowlet to implement in order to receive callback on process result.
 */
public interface Callback {

  /**
   * This method will be called when processing of a given input is completed successfully.
   *
   * @param input The input object that was given to the process method.
   *              If the process method is annotated with {@link com.continuuity.api.annotation.Batch}, the
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
   *              If the process method is annotated with {@link com.continuuity.api.annotation.Batch}, the
   *              input object will be of type {@link java.util.Iterator}, while inside the iterator contains
   *              all event objects sent to the process method within a batch.
   * @param inputContext The {@link InputContext} that was given to the process method.
   * @param reason Reason for the failure.
   * @return A {@link FailurePolicy} indicating how to handle the failure input.
   */
  FailurePolicy onFailure(@Nullable Object input, @Nullable InputContext inputContext, FailureReason reason);
}
