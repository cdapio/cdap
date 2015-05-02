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

package co.cask.cdap.template.etl.api.realtime;

import co.cask.cdap.api.annotation.Beta;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;

/**
 * State of Source.
 */
@Beta
public class SourceState {

  private final Map<String, byte[]> stateMap;

  /**
   * Construct a SourceState using a Map of String keys and byte[] values.
   *
   * @param stateMap {@link Map} of string keys and byte[] values
   */
  public SourceState(Map<String, byte[]> stateMap) {
    this.stateMap = Maps.newHashMap(stateMap);
  }

  /**
   * Construct a SourceState with an empty map.
   */
  public SourceState() {
    this.stateMap = Maps.newHashMap();
  }

  /**
   * Construct a SourceState by making a copy of another SourceState.
   *
   * @param state {@link SourceState}
   */
  public SourceState(SourceState state) {
    this(state.getState());
  }

  /**
   * Get the byte[] associated with a key.
   *
   * @param state state
   * @return byte array
   */
  public byte[] getState(String state) {
    return stateMap.get(state);
  }

  /**
   * Set a single key state given the key and value.
   *
   * @param state key
   * @param value value
   */
  public void setState(String state, byte[] value) {
    stateMap.put(state, value);
  }

  /**
   * Get the full state of the Source.
   *
   * @return {@link Map} of string keys and byte[] values
   */
  public Map<String, byte[]> getState() {
    return Maps.newHashMap(stateMap);
  }

  /**
   * Add a Map to the state of the source.
   *
   * @param map {@link Map} of string keys and byte[] values
   */
  public void setState(Map<String, byte[]> map) {
    stateMap.putAll(map);
  }

  /**
   * Clear the internal state and set it to values from provided State.
   *
   * @param state state to be copied
   */
  public void setState(SourceState state) {
    clearState();
    setState(state.getState());
  }

  /**
   * Clear the internal state.
   */
  public void clearState() {
    stateMap.clear();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SourceState that = (SourceState) o;
    return Objects.equals(stateMap, that.stateMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stateMap);
  }
}
