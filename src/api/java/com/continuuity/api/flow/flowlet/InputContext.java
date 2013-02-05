package com.continuuity.api.flow.flowlet;

/**
 * Represents the context of the input data that is passed
 * to {@link Flowlet} for processing.
 */
public interface InputContext {

  String getName();

  int getRetryCount();
}
