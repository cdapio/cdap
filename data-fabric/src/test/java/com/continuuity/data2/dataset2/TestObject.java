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
