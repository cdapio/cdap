package com.continuuity.hive.objectinspector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector
 * instances.
 * <p/>
 * SerDe classes should call the static functions in this library to create an
 * ObjectInspector to return to the caller of SerDe2.getObjectInspector().
 * <p/>
 * The reason of having caches here is that ObjectInspector is because
 * ObjectInspectors do not have an internal state - so ObjectInspectors with the
 * same construction parameters should result in exactly the same
 * ObjectInspector.
 */
public final class ObjectInspectorFactory {

  private static ConcurrentHashMap<Type, ObjectInspector> objectInspectorCache =
      new ConcurrentHashMap<Type, ObjectInspector>();

  public static ObjectInspector getReflectionObjectInspector(Type t) {
    ObjectInspector oi = objectInspectorCache.get(t);
    if (oi == null) {
      oi = getReflectionObjectInspectorNoCache(t);
      objectInspectorCache.put(t, oi);
    }
    return oi;
  }

  private static ObjectInspector getReflectionObjectInspectorNoCache(Type t) {
    if (t instanceof GenericArrayType) {
      GenericArrayType at = (GenericArrayType) t;
      return getStandardListObjectInspector(getReflectionObjectInspector(at.getGenericComponentType()));
    }

    Map<TypeVariable, Type> genericTypes = null;
    // t has to come from TokenType, otherwise not recognized as ParameterizedType...
    if (t instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType) t;
      // List?
      if (List.class.isAssignableFrom((Class<?>) pt.getRawType())) {
        return getStandardListObjectInspector(getReflectionObjectInspector(pt.getActualTypeArguments()[0]));
      }
      // Map?
      if (Map.class.isAssignableFrom((Class<?>) pt.getRawType())) {
        return getStandardMapObjectInspector(getReflectionObjectInspector(pt.getActualTypeArguments()[0]),
                                             getReflectionObjectInspector(pt.getActualTypeArguments()[1]));
      }
      // Otherwise convert t to RawType so we will fall into the following if block.
      t = pt.getRawType();

      ImmutableMap.Builder<TypeVariable, Type> builder = ImmutableMap.builder();
      for (int i = 0; i < pt.getActualTypeArguments().length; i++) {
        builder.put(((Class<?>) t).getTypeParameters()[i], pt.getActualTypeArguments()[i]);
      }
      genericTypes = builder.build();
    }

    // Must be a class.
    if (!(t instanceof Class)) {
      throw new RuntimeException(ObjectInspectorFactory.class.getName()
          + " internal error:" + t);
    }
    Class<?> c = (Class<?>) t;

    // Java Primitive Type?
    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaType(c)) {
      return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
          PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJavaType(c).primitiveCategory);
    }

    // Java Primitive Class?
    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaClass(c)) {
      return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
          PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveJavaClass(c).primitiveCategory);
    }

    // Primitive Writable class?
    if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(c)) {
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          PrimitiveObjectInspectorUtils.getTypeEntryFromPrimitiveWritableClass(c).primitiveCategory);
    }

    // Enum class?
    if (Enum.class.isAssignableFrom(c)) {
      return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
          PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    // Must be struct because List and Map need to be ParameterizedType
    assert (!List.class.isAssignableFrom(c));
    assert (!Map.class.isAssignableFrom(c));

    Preconditions.checkState(!c.isInterface(), "Cannot inspect an interface.");

    ReflectionStructObjectInspector oi = new ReflectionStructObjectInspector();
    // put it into the cache BEFORE it is initialized to make sure we can catch
    // recursive types.
    objectInspectorCache.put(t, oi);
    Field[] fields = ObjectInspectorUtils.getDeclaredNonStaticFields(c);
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>(
        fields.length);
    for (int i = 0; i < fields.length; i++) {
      if (!oi.shouldIgnoreField(fields[i].getName())) {
        Type newType = fields[i].getGenericType();
        if (newType instanceof TypeVariable) {
          Preconditions.checkNotNull(genericTypes, "Type was not recognized as a parameterized type.");
          Preconditions.checkNotNull(genericTypes.get(newType),
                                     "Generic type " + newType + " not a parameter of class " + c);
          newType = genericTypes.get(newType);
        }
        structFieldObjectInspectors.add(getReflectionObjectInspector(newType));
      }
    }
    oi.init(c, structFieldObjectInspectors);
    return oi;
  }

  static ConcurrentHashMap<ObjectInspector, StandardListObjectInspector> cachedStandardListObjectInspector =
      new ConcurrentHashMap<ObjectInspector, StandardListObjectInspector>();

  public static StandardListObjectInspector getStandardListObjectInspector(ObjectInspector listElementObjectInspector) {
    StandardListObjectInspector result = cachedStandardListObjectInspector.get(listElementObjectInspector);
    if (result == null) {
      result = new StandardListObjectInspector(listElementObjectInspector);
      cachedStandardListObjectInspector.put(listElementObjectInspector, result);
    }
    return result;
  }

  static ConcurrentHashMap<List<ObjectInspector>, StandardMapObjectInspector> cachedStandardMapObjectInspector =
      new ConcurrentHashMap<List<ObjectInspector>, StandardMapObjectInspector>();

  public static StandardMapObjectInspector getStandardMapObjectInspector(ObjectInspector mapKeyObjectInspector,
                                                                         ObjectInspector mapValueObjectInspector) {
    List<ObjectInspector> signature = ImmutableList.of(mapKeyObjectInspector, mapValueObjectInspector);
    StandardMapObjectInspector result = cachedStandardMapObjectInspector.get(signature);
    if (result == null) {
      result = new StandardMapObjectInspector(mapKeyObjectInspector, mapValueObjectInspector);
      cachedStandardMapObjectInspector.put(signature, result);
    }
    return result;
  }

  private ObjectInspectorFactory() {
    // prevent instantiation
  }

}

