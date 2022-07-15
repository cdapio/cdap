/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.inject.Inject;
import io.cdap.cdap.app.runtime.TwillControllerCreator;
import io.cdap.cdap.app.runtime.TwillControllerCreatorFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Implementation of {@link TwillControllerCreatorFactory} which returns {@link TwillControllerCreator} based on
 * {@link ClusterMode}
 */
public final class DefaultTwillControllerCreatorFactory implements TwillControllerCreatorFactory {

  private final Map<ClusterMode, TwillControllerCreator> twillControllerCreatorMap;

  @Inject
  DefaultTwillControllerCreatorFactory(Map<ClusterMode, TwillControllerCreator> twillControllerCreatorMap) {
    this.twillControllerCreatorMap = twillControllerCreatorMap;
  }

  @Override
  public TwillControllerCreator create(ClusterMode clusterMode) {
    return Optional.ofNullable(twillControllerCreatorMap).map(map -> map.get(clusterMode))
      .orElseThrow(UnsupportedOperationException::new);
  }
}
