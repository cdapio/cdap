package co.cask.cdap.templates.etl.common;

import co.cask.cdap.templates.etl.api.ValueEmitter;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Default implementation of {@link ValueEmitter}.
 */
public class DefaultValueEmitter implements ValueEmitter, Iterable {
  private final List<Object> valueList;

  public DefaultValueEmitter() {
    this.valueList = Lists.newArrayList();
  }

  @Override
  public void emit(Object value) {
    valueList.add(value);
  }

  @Override
  public void emit(Object key, Object value) {
    valueList.add(value);
  }

  @Override
  public Iterator iterator() {
    return valueList.iterator();
  }

  public void reset() {
    valueList.clear();
  }
}
