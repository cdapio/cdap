/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.lang;

import com.continuuity.internal.lang.Reflections;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ReflectionsTest {

  @Test
  public void testResolved() throws Exception {
    Assert.assertTrue(Reflections.isResolved(String.class));
    Assert.assertTrue(Reflections.isResolved(new TypeToken<Map<String, Set<Integer>>>() { }.getType()));

    TypeToken<Record<Set<Integer>>> typeToken = new TypeToken<Record<Set<Integer>>>() { };
    Type arrayType = Record.class.getMethod("getArray").getGenericReturnType();
    Assert.assertFalse(Reflections.isResolved(arrayType));
    Assert.assertTrue(Reflections.isResolved(typeToken.resolveType(arrayType).getType()));
  }

  private interface Record<T> {

    T get();

    T[] getArray();
  }
}
