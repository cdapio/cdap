package com.continuuity.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;

import java.util.HashMap;
import java.util.Map;

/**
 * StandardMapObjectInspector works on map data that is stored as a Java Map
 * object. Note: the key object of the map must support equals and hashCode by
 * itself.
 * 
 * We also plan to have a GeneralMapObjectInspector which can work on map with
 * key objects that does not support equals and hashCode. That will require us
 * to store InspectableObject as the key, which will have overridden equals and
 * hashCode methods.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 */
public class StandardMapObjectInspector implements SettableMapObjectInspector {

  ObjectInspector mapKeyObjectInspector;
  ObjectInspector mapValueObjectInspector;

  /**
   * Call ObjectInspectorFactory.getStandardMapObjectInspector instead.
   * Because of this protected constructor in the orignal Hive class, we cannot reuse it.
   */
  protected StandardMapObjectInspector(ObjectInspector mapKeyObjectInspector, ObjectInspector mapValueObjectInspector) {
    this.mapKeyObjectInspector = mapKeyObjectInspector;
    this.mapValueObjectInspector = mapValueObjectInspector;
  }

  // without data
  public ObjectInspector getMapKeyObjectInspector() {
    return mapKeyObjectInspector;
  }

  public ObjectInspector getMapValueObjectInspector() {
    return mapValueObjectInspector;
  }

  // with data
  // TODO: Now we assume the key Object supports hashCode and equals functions.
  public Object getMapValueElement(Object data, Object key) {
    if (data == null || key == null) {
      return null;
    }
    Map<?, ?> map = (Map<?, ?>) data;
    return map.get(key);
  }

  public int getMapSize(Object data) {
    if (data == null) {
      return -1;
    }
    Map<?, ?> map = (Map<?, ?>) data;
    return map.size();
  }

  public Map<?, ?> getMap(Object data) {
    if (data == null) {
      return null;
    }
    Map<?, ?> map = (Map<?, ?>) data;
    return map;
  }

  public final Category getCategory() {
    return Category.MAP;
  }

  public String getTypeName() {
    return org.apache.hadoop.hive.serde.serdeConstants.MAP_TYPE_NAME + "<"
        + mapKeyObjectInspector.getTypeName() + ","
        + mapValueObjectInspector.getTypeName() + ">";
  }

  // /////////////////////////////
  // SettableMapObjectInspector
  @Override
  public Object create() {
    Map<Object, Object> m = new HashMap<Object, Object>();
    return m;
  }

  @Override
  public Object clear(Object map) {
    Map<Object, Object> m = (HashMap<Object, Object>) map;
    m.clear();
    return m;
  }

  @Override
  public Object put(Object map, Object key, Object value) {
    Map<Object, Object> m = (HashMap<Object, Object>) map;
    m.put(key, value);
    return m;
  }

  @Override
  public Object remove(Object map, Object key) {
    Map<Object, Object> m = (HashMap<Object, Object>) map;
    m.remove(key);
    return m;
  }

}
