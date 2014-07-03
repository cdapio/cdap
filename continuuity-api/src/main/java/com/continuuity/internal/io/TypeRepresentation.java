package com.continuuity.internal.io;

import com.google.common.base.Objects;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
   * Helper class to represent a type parameter in a serializable form. It would be simple if we could just use 
   * the class name, but we would lose the type parameters of generic classes. If we'd use java.lang.reflect.Type,
   * we'd not be able to serialize easily. So here we implement our own representation of the type: it supports 
   * classes and parametrized classes, it also supports static inner classes. However, it does not support 
   * interfaces (yet). 
   * 
   * This class can be serialized to Json and deserialized back without loss. Because it implements ParametrizedType,
   * this class is compatible with TypeToken and we can use it to decode an encoded object of this type. 
 */
public final class TypeRepresentation implements ParameterizedType {

  private final boolean isClass;
  private final String rawType;
  private final TypeRepresentation enclosingType;
  private final TypeRepresentation[] parameters;
  private transient ClassLoader classLoader = null;

  /**
   * Set the class loader to be used by toType(), getRawType(), etc.
   * @param loader the class loader to use
   */
  public void setClassLoader(ClassLoader loader) {
    this.classLoader = loader;
    // Note that this class is immutable after construction, except for the class loader
    // Thus we can pass down the loader to the owner and parameter types once now, no need to pass it down in toType()
    if (this.enclosingType != null) {
      this.enclosingType.setClassLoader(loader);
    }
    if (this.parameters != null) {
      for (TypeRepresentation param: this.parameters) {
        param.setClassLoader(loader);
      }
    }
  }

  /**
   * Constructor from a java Type. For a class, we only remember its name; for a parametrized type, we remember the
   * name, the enclosing class, and the type parameters.
   * @param type The type to represent
   * @throws UnsupportedTypeException
   */
  public TypeRepresentation(Type type) throws UnsupportedTypeException {
    if (type instanceof Class<?>) {
      this.rawType = ((Class) type).getCanonicalName();
      this.enclosingType = null;
      this.parameters = null;
      this.isClass = true;
    } else if (type instanceof ParameterizedType) {
      ParameterizedType pType = (ParameterizedType) type;
      Type raw = pType.getRawType();
      if (raw instanceof Class<?>) {
        this.rawType = ((Class) raw).getName();
      } else {
        throw new UnsupportedTypeException("can't represent type " + type + " (enclosing type is not a class)");
      }
      Type owner = pType.getOwnerType();
      this.enclosingType = owner == null ?  null : new TypeRepresentation(owner);
      Type[] typeArgs = pType.getActualTypeArguments();
      this.parameters = new TypeRepresentation[typeArgs.length];
      for (int i = 0; i < typeArgs.length; i++) {
        this.parameters[i] = new TypeRepresentation(typeArgs[i]);
      }
      this.isClass = false;
    } else {
      throw new UnsupportedTypeException("can't represent type " + type + " (must be a class or a parametrized type)");
    }
  }

  /**
   * @return the represented Type. Note that for a parametrized type, we can just return this,
   * because it implements the ParametrizedType interface.
   */
  public Type toType() {
    if (this.isClass) {
      return this.getRawType();
    } else {
      return this;
    }
  }

  @Override
  public Type[] getActualTypeArguments() {
    if (this.parameters == null) {
      return null;
    }
    Type[] typeArgs = new Type[this.parameters.length];
    for (int i = 0; i < typeArgs.length; i++) {
      typeArgs[i] = this.parameters[i].toType();
    }
    return typeArgs;
  }

  @Override
  public Type getRawType() {
    try {
      ClassLoader cl = Objects.firstNonNull(classLoader,
                                            Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                                 getClass().getClassLoader()));
      return cl.loadClass(this.rawType);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("cannot convert " + this.rawType + " to a type. ", e);
    }
  }

  @Override
  public Type getOwnerType() {
    return this.enclosingType == null ? null : this.enclosingType.toType();
  }
} // TypeRepresentation
