package com.continuuity.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;

import java.util.ArrayList;
import java.util.Collection;
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
    // We support both List<Object> and Object[]
    // so we have to do differently.
    boolean isArray = !(data instanceof List);
    if (isArray) {
      Object[] list = (Object[]) data;
      if (index < 0 || index >= list.length) {
        return null;
      }
      return list[index];
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
    // We support both List<Object> and Object[]
    // so we have to do differently.
    boolean isArray = !(data instanceof List);
    if (isArray) {
      Object[] list = (Object[]) data;
      return list.length;
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
        // NOTE since the parameter of the collection is lost at runtime,
        // we cannot use new ArrayList<?>(data)
        data = java.util.Arrays.asList(((Collection) data).toArray());
      } else {
        data = java.util.Arrays.asList((Object[]) data);
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
    List<Object> a = (List<Object>) list;
    while (a.size() < newSize) {
      a.add(null);
    }
    while (a.size() > newSize) {
      a.remove(a.size() - 1);
    }
    return a;
  }

  @Override
  public Object set(Object list, int index, Object element) {
    List<Object> a = (List<Object>) list;
    a.set(index, element);
    return a;
  }

}
