package com.continuuity.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;

import java.util.List;

/**
 * StandardUnionObjectInspector works on union data that is stored as
 * UnionObject.
 * It holds the list of the object inspectors corresponding to each type of the
 * object the Union can hold. The UniobObject has tag followed by the object
 * it is holding.
 *
 * Always use the {@link org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory}
 * to create new ObjectInspector objects, instead of directly creating an instance of this class.
 */
public class StandardUnionObjectInspector implements UnionObjectInspector {
  List<ObjectInspector> ois;

  public StandardUnionObjectInspector(List<ObjectInspector> ois) {
    this.ois = ois;
  }

  public List<ObjectInspector> getObjectInspectors() {
    return ois;
  }

  /**
   * Standard Union.
   */
  public static class StandardUnion implements UnionObject {
    protected byte tag;
    protected Object object;

    public StandardUnion() {
    }

    public StandardUnion(byte tag, Object object) {
      this.tag = tag;
      this.object = object;
    }

    public void setObject(Object o) {
      this.object = o;
    }

    public void setTag(byte tag) {
      this.tag = tag;
    }

    @Override
    public Object getObject() {
      return object;
    }

    @Override
    public byte getTag() {
      return tag;
    }

    @Override
    public String toString() {
      return tag + ":" + object;
    }
  }

  /**
   * Return the tag of the object.
   */
  public byte getTag(Object o) {
    if (o == null) {
      return -1;
    }
    return ((UnionObject) o).getTag();
  }

  /**
   * Return the field based on the tag value associated with the Object.
   */
  public Object getField(Object o) {
    if (o == null) {
      return null;
    }
    return ((UnionObject) o).getObject();
  }

  public Category getCategory() {
    return Category.UNION;
  }

  public String getTypeName() {
    return ObjectInspectorUtils.getStandardUnionTypeName(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getName());
    sb.append(getTypeName());
    return sb.toString();
  }

}
