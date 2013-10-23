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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

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
        if (!Modifier.isStatic(field.getModifiers())) {
          Assert.assertEquals(Defaults.defaultValue(field.getType()), field.get(instance));
        }
      }
    });
  }

  public static final class Record {
    private static final Logger LOG = LoggerFactory.getLogger(Record.class);

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
