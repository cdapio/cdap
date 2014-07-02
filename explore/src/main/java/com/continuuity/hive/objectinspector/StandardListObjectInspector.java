package com.continuuity.hive.objectinspector;

import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * DefaultListObjectInspector works on list data that is stored as a Java List
 * or Java Array object.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 */
public class StandardListObjectInspector implements SettableListObjectInspector {

  ObjectInspector listElementObjectInspector;

  /**
   * Call ObjectInspectorFactory.getStandardListObjectInspector instead.
   * Because of this protected constructor in the orignal Hive class, we cannot reuse it.
   */
  protected StandardListObjectInspector(ObjectInspector listElementObjectInspector) {
    this.listElementObjectInspector = listElementObjectInspector;
  }

  public final Category getCategory() {
    return Category.LIST;
  }

  // without data
  public ObjectInspector getListElementObjectInspector() {
    return listElementObjectInspector;
  }

  // with data
  public Object getListElement(Object data, int index) {
    if (data == null) {
      return null;
    }
    // We support both List<Object>, Object[] and Collection<Object>
    // so we have to do differently.
    if (!(data instanceof List)) {
      if (data instanceof Collection) {
        Collection<?> collection = (Collection<?>) data;
        if (index < 0 || index >= collection.size()) {
          return null;
        }
        Iterator<?> ite = collection.iterator();
        for (int i = 0; i < index; i++) {
          if (!ite.hasNext()) {
            return null;
          }
          ite.next();
        }
        if (!ite.hasNext()) {
          return null;
        }
        return ite.next();
      } else {
        List<?> list = getList(data);
        if (index < 0 || index >= list.size()) {
          return null;
        }
        return list.get(index);
      }
    } else {
      List<?> list = (List<?>) data;
      if (index < 0 || index >= list.size()) {
        return null;
      }
      return list.get(index);
    }
  }

  public int getListLength(Object data) {
    if (data == null) {
      return -1;
    }
    // We support both List<Object>, Object[] and Collection<Object>
    // so we have to do differently.
    if (!(data instanceof List)) {
      if (data instanceof Collection) {
        return ((Collection) data).size();
      } else {
        List<?> list = getList(data);
        return list.size();
      }
    } else {
      List<?> list = (List<?>) data;
      return list.size();
    }
  }

  public List<?> getList(Object data) {
    if (data == null) {
      return null;
    }
    // We support List<Object>, Object[], and Collection<Object>
    // so we have to do differently.
    if (!(data instanceof List)) {
      if (data instanceof Collection) {
        data = Lists.newArrayList((Collection<?>) data);
      } else if (data instanceof Object[]) {
        data = java.util.Arrays.asList((Object[]) data);
      } else if (data instanceof byte[]) {
        data = Bytes.asList((byte[]) data);
      } else if (data instanceof int[]) {
        data = Ints.asList((int[]) data);
      } else if (data instanceof double[]) {
        data = Doubles.asList((double[]) data);
      } else if (data instanceof float[]) {
        data = Floats.asList((float[]) data);
      } else if (data instanceof short[]) {
        data = Shorts.asList((short[]) data);
      } else if (data instanceof long[]) {
        data = Longs.asList((long[]) data);
      } else if (data instanceof boolean[]) {
        data = Booleans.asList((boolean[]) data);
      } else {
        throw new UnsupportedOperationException("Data object is neither a Collection nor an array.");
      }
    }
    List<?> list = (List<?>) data;
    return list;
  }

  public String getTypeName() {
    return org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "<" +
        listElementObjectInspector.getTypeName() + ">";
  }

  // /////////////////////////////
  // SettableListObjectInspector
  @Override
  public Object create(int size) {
    List<Object> a = new ArrayList<Object>(size);
    for (int i = 0; i < size; i++) {
      a.add(null);
    }
    return a;
  }

  @Override
  public Object resize(Object list, int newSize) {
    if (!(list instanceof List) && (list instanceof Collection)) {
      // NOTE: no use-case should enter this statement,
      // because the list comes from the create method
      // but it's better to be safe than sorry.
      list = Lists.newArrayList((Collection<?>) list);
    }
    List<Object> a = (List<Object>) list;
    while (a.size() < newSize) {
      a.add(null);
    }
    while (a.size() > newSize) {
      a.remove(a.size() - 1);
    }
    // Return the modified list, but the list param will not be modified
    return a;
  }

  @Override
  public Object set(Object list, int index, Object element) {
    if (!(list instanceof List) && (list instanceof Collection)) {
      // NOTE: no use-case should enter this statement,
      // because the list comes from the create method
      // but it's better to be safe than sorry.
      list = Lists.newArrayList((Collection<?>) list);
    }
    List<Object> a = (List<Object>) list;
    a.set(index, element);
    // Return the modified list, but the list param will not be modified
    return a;
  }

}
