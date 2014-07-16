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

package com.continuuity.data2.dataset2;

import com.google.common.base.Objects;

public final class TestObject {
  private final String a;
  private final long b;

  public TestObject(String a, long b) {
    this.a = a;
    this.b = b;
  }

  public String getA() {
    return a;
  }

  public long getB() {
    return b;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("TestObject{");
    sb.append("a='").append(a).append('\'');
    sb.append(", b=").append(b);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TestObject that = (TestObject) o;

    return b == that.b
      && Objects.equal(a, that.a);
  }
}
