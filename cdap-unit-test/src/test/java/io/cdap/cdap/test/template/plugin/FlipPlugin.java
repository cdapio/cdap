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

package io.cdap.cdap.test.template.plugin;

import com.google.common.base.Function;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import javax.annotation.Nullable;

/**
 * Template plugin for tests
 */
@Plugin(type = "function")
@Name("flip")
public class FlipPlugin implements Function<Long, Long> {

  @Nullable
  @Override
  public Long apply(Long input) {
    return 0 - input;
  }

}
