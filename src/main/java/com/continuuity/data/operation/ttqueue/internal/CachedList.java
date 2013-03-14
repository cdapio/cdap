package com.continuuity.data.operation.ttqueue.internal;

import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class CachedList<T> {
  final private List<T> list;
  private int nextIndex;

  public static final CachedList EMPTY_LIST = new CachedList(Collections.emptyList());
  public static <T> CachedList<T> emptyList() {
    return EMPTY_LIST;
  }

  public CachedList(List<T> list) {
    this.list = list;
    this.nextIndex = 0;
  }

  public T getNext() {
    if(nextIndex >= list.size()) {
      throw new IllegalStateException(String.format("Out of bounds access(%d) of cached list, size of list = %d", nextIndex, list.size()));
    }
    return list.get(nextIndex++);
  }

  public T getCurrent() {
    int currentIndex = nextIndex - 1;
    if(currentIndex >= list.size()) {
      throw new IllegalStateException(String.format("Out of bounds access(%d) of cached list, size of list = %d", currentIndex, list.size()));
    }
    return list.get(currentIndex);
  }

  public boolean hasCurrent() {
    return nextIndex > 0;
  }

  public boolean hasNext() {
    return nextIndex < list.size();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("list", Arrays.toString(list.toArray()))
      .add("nextIndex", nextIndex)
      .toString();
  }
}
