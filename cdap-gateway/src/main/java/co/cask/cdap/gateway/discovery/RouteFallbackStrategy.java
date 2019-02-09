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

package co.cask.cdap.gateway.discovery;

/**
 * Fall back routing strategy for User Services.
 */
public enum RouteFallbackStrategy {
  RANDOM,
  DROP,
  SMALLEST,
  LARGEST;

  public static RouteFallbackStrategy valueOfRouteFallbackStrategy(String strategy) {
    try {
      return RouteFallbackStrategy.valueOf(strategy.toUpperCase());
    } catch (Exception e) {
      return RouteFallbackStrategy.RANDOM;
    }
  }
}
