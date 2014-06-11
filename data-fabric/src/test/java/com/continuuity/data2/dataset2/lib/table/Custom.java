package com.continuuity.data2.dataset2.lib.table;

import java.util.ArrayList;

public final class Custom {
  int i;
  ArrayList<String> sl;
  Custom(int i, ArrayList<String> sl) {
    this.i = i;
    this.sl = sl;
  }
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || o.getClass() != this.getClass()) {
      return false;
    }
    if (this.i != ((Custom) o).i) {
      return false;
    }
    if (this.sl == null) {
      return ((Custom) o).sl == null;
    }
    return this.sl.equals(((Custom) o).sl);
  }
  @Override
  public int hashCode() {
    return 31 * i + (sl != null ? sl.hashCode() : 0);
  }
}
