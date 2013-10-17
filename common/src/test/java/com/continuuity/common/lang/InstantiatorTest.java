/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.lang;

import com.continuuity.internal.lang.FieldVisitor;
import com.continuuity.internal.lang.Reflections;
import com.google.common.base.Defaults;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 *
 */
public class InstantiatorTest {

  @Test
  public void testUnsafe() {
    Record record = new InstantiatorFactory(false).get(TypeToken.of(Record.class)).create();

    Reflections.visit(record, TypeToken.of(Record.class), new FieldVisitor() {
      @Override
      public void visit(Object instance, TypeToken<?> inspectType,
                        TypeToken<?> declareType, Field field) throws Exception {

        Assert.assertEquals(Defaults.defaultValue(field.getType()), field.get(instance));
      }
    });
  }

  public static final class Record {
    private boolean bool;
    private byte b;
    private char c;
    private short s;
    private int i;
    private long l;
    private float f;
    private double d;
    private String str;

    // Just to avoid having default constructor.
    public Record(String s) {
    }
  }
}
