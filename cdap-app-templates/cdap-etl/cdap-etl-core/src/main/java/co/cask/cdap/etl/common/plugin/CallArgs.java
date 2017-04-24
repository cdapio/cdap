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

package co.cask.cdap.etl.common.plugin;

/**
 * Arguments used by a {@link Caller} when running a Callable. Right now it only has a flag for whether or not
 * timing should be skipped, but it is here in case we add more functionality. For example, if we decide we want to
 * have separate timers for different methods.
 *
 * This is used right now because we want to time some methods but not others. For example, most plugins have some
 * lifecycle methods like initialize() and destroy() that are only called once. We don't want to track time for those
 * methods because they are just called once and will probably throw off the min time and standard deviation metrics
 * that we want to track.
 */
public class CallArgs {
  public static final CallArgs NONE = builder().build();
  public static final CallArgs TRACK_TIME = builder().trackTime().build();
  private final boolean trackTime;

  private CallArgs(boolean trackTime) {
    this.trackTime = trackTime;
  }

  public boolean shouldTrackTime() {
    return trackTime;
  }

  /**
   * @return a builder to call args
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds caller args.
   */
  public static class Builder {
    private boolean trackTime = false;

    public Builder trackTime() {
      trackTime = true;
      return this;
    }

    public CallArgs build() {
      return new CallArgs(trackTime);
    }
  }
}
