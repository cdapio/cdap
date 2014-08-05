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

package co.cask.cdap.common.zookeeper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.util.List;
import java.util.Set;

/**
 * Utilities for creating Zookeeper ACLs.
 */
public class ZKACLs {

  public static List<ACL> fromSaslPrincipalsAllowAll(String... principals) {
    Set<String> existingPrincipals = Sets.newHashSet();
    ImmutableList.Builder<ACL> result = ImmutableList.builder();
    for (String principal : principals) {
      if (principal != null && !existingPrincipals.contains(principal)) {
        result.add(new ACL(ZooDefs.Perms.ALL, ZKIds.createSaslId(principal)));
        existingPrincipals.add(principal);
      }
    }
    return result.build();
  }

}
