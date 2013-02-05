package com.continuuity.api.flow.flowlet;

/**
 *
 */
public interface OutputEmitter<T> {

  void emit(T data);
}
