package com.continuuity.api.dataset.lib;

/**
 * Represents key value pair
 * @param <KEY_TYPE> type of the key
 * @param <VALUE_TYPE> type of the value
 */
public class KeyValue<KEY_TYPE, VALUE_TYPE> {
  private final KEY_TYPE key;
  private final VALUE_TYPE value;

  public KeyValue(KEY_TYPE key, VALUE_TYPE value) {
    this.key = key;
    this.value = value;
  }

  public KEY_TYPE getKey() {
    return key;
  }

  public VALUE_TYPE getValue() {
    return value;
  }
}
