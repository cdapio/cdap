package com.continuuity.io;

import com.continuuity.internal.io.ASMFieldAccessorFactory;
import com.continuuity.internal.io.FieldAccessor;
import com.continuuity.internal.io.FieldAccessorFactory;
import com.google.common.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class FieldAccessorTest {

  public static class Parent<T> {
    private T value;
  }

  public static class Child extends Parent<String> {
    protected String str;
    int integer;
  }

  @Test
  public void testAccessor() throws Exception {
    TypeToken<Child> type = TypeToken.of(Child.class);

    FieldAccessorFactory factory = new ASMFieldAccessorFactory();
    FieldAccessor accessor = factory.getFieldAccessor(type, "integer");

    Child c = new Child();
    c.integer = 10;
    ((Parent)c).value = "string value";
    Assert.assertEquals(c.integer, accessor.getInt(c));

    Assert.assertSame(accessor, factory.getFieldAccessor(type, "integer"));

    accessor = factory.getFieldAccessor(type, "value");
    Assert.assertSame(((Parent)c).value, accessor.get(c));
  }
}
