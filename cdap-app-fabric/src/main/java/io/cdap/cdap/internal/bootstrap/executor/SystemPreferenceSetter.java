/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.bootstrap.executor;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.config.PreferencesService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Sets system preferences if they don't already exist.
 */
public class SystemPreferenceSetter extends BaseStepExecutor<SystemPreferenceSetter.Arguments> {
  private final PreferencesService preferencesService;

  @Inject
  SystemPreferenceSetter(PreferencesService preferencesService) {
    this.preferencesService = preferencesService;
  }

  @Override
  public void execute(Arguments arguments) throws BadRequestException, NotFoundException, ProfileConflictException {
    try {
      preferencesService.addProperties(arguments.getPreferences());
    } catch (RuntimeException e) {
      // runtime exceptions are those propagated by some lower layer, usually due to some transaction failure
      // these should be safe to retry, at least up to the default time limit
      // as always, it is almost impossible to do this accurately without refactoring a bunch of layers below
      throw new RetryableException(e);
    }
  }

  /**
   * Arguments required to set system preferences
   */
  static class Arguments implements Validatable {
    private Map<String, String> preferences;

    @VisibleForTesting
    Arguments(Map<String, String> preferences) {
      this.preferences = Collections.unmodifiableMap(new HashMap<>(preferences));
    }

    Map<String, String> getPreferences() {
      return preferences == null ? Collections.emptyMap() : preferences;
    }

    @Override
    public void validate() {
      // no-op
    }
  }
}
