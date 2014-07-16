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
package com.continuuity.common.zookeeper.coordination;

import com.continuuity.api.common.Bytes;
import com.google.common.primitives.Ints;
import org.apache.twill.discovery.Discoverable;

import java.net.InetSocketAddress;
import java.util.Comparator;

/**
 * A {@link Comparator} for {@link Discoverable}.
 *
 * Note: This class may move to other package when needed.
 */
public final class DiscoverableComparator implements Comparator<Discoverable> {

  public static final Comparator<Discoverable> COMPARATOR = new DiscoverableComparator();

  @Override
  public int compare(Discoverable o1, Discoverable o2) {
    int cmp = o1.getName().compareTo(o2.getName());
    if (cmp != 0) {
      return cmp;
    }

    InetSocketAddress address1 = o1.getSocketAddress();
    InetSocketAddress address2 = o2.getSocketAddress();
    cmp = Bytes.compareTo(address1.getAddress().getAddress(), address2.getAddress().getAddress());

    return (cmp == 0) ? Ints.compare(address1.getPort(), address2.getPort()) : cmp;
  }
}
