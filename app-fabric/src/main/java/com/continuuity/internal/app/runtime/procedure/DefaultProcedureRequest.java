package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureRequest;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.internal.Primitives;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Map;

/**
 *
 */
final class DefaultProcedureRequest implements ProcedureRequest {

  private final String method;
  private final Map<String, String> arguments;

  DefaultProcedureRequest(String method, Map<String, String> arguments) {
    this.method = method;
    this.arguments = ImmutableMap.copyOf(arguments);
  }

  @Override
  public String getMethod() {
    return method;
  }

  @Override
  public Map<String, String> getArguments() {
    return arguments;
  }

  @Override
  public String getArgument(String key) {
    return arguments.get(key);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getArgument(String key, Class<T> type) {
    Preconditions.checkNotNull(type, "Type cannnot be null.");
    Preconditions.checkArgument(!Void.class.equals(type) && !Void.TYPE.equals(type), "Void type not supported.");

    String value = getArgument(key);

    if (String.class.equals(type)) {
      return (T) value;
    }

    Class<T> resolvedType = type;
    if (Primitives.isPrimitive(resolvedType)) {
      resolvedType = Primitives.wrap(type);
    }
    if (Primitives.isWrapperType(resolvedType)) {
      // All wrapper has the valueOf(String) method
      try {
        return (T) resolvedType.getMethod("valueOf", String.class).invoke(null, value);
      } catch (Exception e) {
        // Should not happen
        throw Throwables.propagate(e);
      }
    }
    if (URL.class.equals(type)) {
      try {
        return (T) new URL(value);
      } catch (MalformedURLException e) {
        throw Throwables.propagate(e);
      }
    }
    if (URI.class.equals(type)) {
      return (T) URI.create(value);
    }

    // TODO: Maybe support gson decode the type??
    throw new ClassCastException("Fail to convert from String to " + type);
  }
}
