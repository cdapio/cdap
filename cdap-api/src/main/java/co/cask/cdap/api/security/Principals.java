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

package co.cask.cdap.api.security;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Utility functions for {@link Principals}.
 */
public final class Principals {

  private Principals() { }

  public static List<Principal> fromIds(PrincipalType type, List<String> ids) {
    List<Principal> result = Lists.newArrayList();
    for (String id : ids) {
      result.add(new Principal(type, id));
    }
    return result;
  }
}
