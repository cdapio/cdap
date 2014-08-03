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

package co.cask.cdap.data2.dataset2.lib.table;

import com.google.common.collect.Sets;

import java.util.SortedSet;

public final class CustomWithInner<T> {
  T a;
  CustomWithInner(T t) {
    this.a = t;
  }
  public static class Inner<U> {
    SortedSet<Integer> set;
    U x;
    Inner(int i, U u) {
      this.set = Sets.newTreeSet();
      this.set.add(i);
      this.x = u;
    }
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Inner inner = (Inner) o;
      if (set != null ? !set.equals(inner.set) : inner.set != null) {
        return false;
      }
      if (x != null ? !x.equals(inner.x) : inner.x != null) {
        return false;
      }
      return true;
    }
    @Override
    public int hashCode() {
      int result = set != null ? set.hashCode() : 0;
      result = 31 * result + (x != null ? x.hashCode() : 0);
      return result;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CustomWithInner that = (CustomWithInner) o;
    if (a != null ? !a.equals(that.a) : that.a != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return a != null ? a.hashCode() : 0;
  }
}
