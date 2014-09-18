/*
 * Copyright © 2014 Cask Data, Inc.
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

/**
 * Represents an expression that can match a key,value in MDC.
 */
public class MdcExpression implements Filter {
  private final String key;
  private final String value;

  public MdcExpression(String key, String value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public boolean match(ILoggingEvent event) {
    String value = event.getMDCPropertyMap().get(getKey());
    return value != null && value.equals(getValue());
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("key", key)
      .add("value", value)
      .toString();
  }
}
