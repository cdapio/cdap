/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.logging.filter;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Represents an And Filter where all sub expressions are and-ed together.
 */
public class AndFilter implements Filter {
  private final List<? extends Filter> expressions;

  public AndFilter(List<? extends Filter> expressions) {
    this.expressions = ImmutableList.copyOf(expressions);
  }

  @Override
  public boolean match(ILoggingEvent event) {
    for (Filter expression : expressions) {
      if (!expression.match(event)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("expressions", expressions)
      .toString();
  }
}
