/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.service;

import com.continuuity.api.service.ServiceSpecification;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillSpecification;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class defines a specification for a {@link com.continuuity.api.service.ServiceSpecification}.
 */
public class DefaultServiceSpecification implements ServiceSpecification {

  private final TwillSpecification specification;
  private final String className;

  public DefaultServiceSpecification(String className, TwillSpecification specification) {
    this.specification = specification;
    this.className = className;
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return specification.getName();
  }

  @Override
  public Map<String, RuntimeSpecification> getRunnables() {
    return specification.getRunnables();
  }

  @Override
  public List<Order> getOrders() {
    return specification.getOrders();
  }

  @Nullable
  @Override
  public EventHandlerSpecification getEventHandler() {
    return specification.getEventHandler();
  }

  @Override
  public String getDescription() {
    return "";
  }
}
