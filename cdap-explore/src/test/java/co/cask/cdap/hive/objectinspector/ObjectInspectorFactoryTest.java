/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.hive.objectinspector;

import co.cask.cdap.common.utils.ImmutablePair;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ObjectInspectorFactoryTest {

  private String getObjectName(Type t) {
    ObjectInspector objectInspector =
        ObjectInspectorFactory.getReflectionObjectInspector(t);
    return objectInspector.getTypeName();
  }

  private void assertObjectInspection(Type t, Object data) throws Exception {
    Field[] tmpFields;
    // Build the expected fields, based on the type t. Exclude the transient fields.
    if (t instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType) t;
      // TODO either test getDeclaredNonStaticFields or use another method
      tmpFields =  ObjectInspectorUtils.getDeclaredNonStaticFields((Class<?>) pt.getRawType());
    } else {
      tmpFields = ObjectInspectorUtils.getDeclaredNonStaticFields((Class<?>) t);
    }
    ImmutableList.Builder builder = ImmutableList.builder();
    for (Field f : tmpFields) {
      if (!Modifier.isTransient(f.getModifiers()) && !f.isSynthetic()) {
        builder.add(f);
      }
    }
    List<Field> expectedFields = builder.build();

    ObjectInspector oi1 = ObjectInspectorFactory.getReflectionObjectInspector(t);
    ObjectInspector oi2 = ObjectInspectorFactory.getReflectionObjectInspector(t);
    Assert.assertEquals(oi1, oi2);

    // metadata
    Assert.assertEquals(ObjectInspector.Category.STRUCT, oi1.getCategory());
    StructObjectInspector soi = (StructObjectInspector) oi1;
    List<? extends StructField> inspectorFields = soi.getAllStructFieldRefs();
    Assert.assertEquals(expectedFields.size(), inspectorFields.size());

    // null
    for (int i = 0; i < inspectorFields.size(); i++) {
      Assert.assertNull(soi.getStructFieldData(null, inspectorFields.get(i)));
    }
    Assert.assertNull(soi.getStructFieldsDataAsList(null));

    // non nulls
    ArrayList<Object> afields = new ArrayList<Object>();
    for (int i = 0; i < expectedFields.size(); i++) {
      Assert.assertEquals(expectedFields.get(i).get(data), soi.getStructFieldData(data, inspectorFields.get(i)));
      afields.add(soi.getStructFieldData(data, inspectorFields.get(i)));
    }
    Assert.assertEquals(afields, soi.getStructFieldsDataAsList(data));
  }

  @Test
  public void reflectionObjectInspectorTest() throws Exception {
    Assert.assertEquals("array<string>", getObjectName(new TypeToken<List<String>>() { }.getType()));
    Assert.assertEquals("array<struct<address:struct<street:string>>>",
                        getObjectName(new TypeToken<List<DummyEmployee<DummyAddress<String>>>>() { }.getType()));
    Assert.assertEquals("array<string>",
                        getObjectName(new TypeToken<ArrayList<String>>() { }.getType()));
    Assert.assertEquals("struct<first:array<string>,second:int>",
                        getObjectName(new TypeToken<ImmutablePair<ImmutableList<String>, Integer>>() { }.getType()));
    Assert.assertEquals("struct<address:struct<street:string>>",
                        getObjectName(new TypeToken<DummyEmployee<DummyAddress<String>>>() { }.getType()));
    Assert.assertEquals("struct<myint:int,myinteger:int,mystring:string,dummystruct:this," +
                        "myliststring:array<string>,mymapstringstring:map<string,string>," +
                        "employee:struct<address:struct<street:string>>,ints:array<int>>",
                        getObjectName(DummyStruct.class));

    // Make sure we don't have infinite loop with nested classes
    Assert.assertEquals("struct<i:int>",
                        getObjectName(DummyParentStruct.DummyInnerClass.class));
    Assert.assertEquals("struct<innerclass:struct<i:int>>",
                        getObjectName(DummyParentStruct.class));


    DummyStruct a = new DummyStruct();
    a.myInt = 1;
    a.myInteger = 2;
    a.myString = "test";
    a.dummyStruct = a;
    a.myListString = Arrays.asList(new String[]{"a", "b", "c"});
    a.myMapStringString = new HashMap<String, String>();
    a.myMapStringString.put("key", "value");
    a.employee = new DummyEmployee<DummyAddress<String>>(new DummyAddress<String>("foo"));
    a.ints = new int[] { 1, 2 };

    assertObjectInspection(DummyStruct.class, a);

    // NOTE: type has to come from TokenType, otherwise, if doing new DummyEmployee<...>().getClass(),
    // type will not be recognized as ParameterizedType
    assertObjectInspection(new TypeToken<DummyEmployee<DummyAddress<String>>>() { }.getType(),
                           new DummyEmployee<DummyAddress<String>>(new DummyAddress<String>("foo")));
  }

  ////////////// Dummy classes used for this class test /////////////
  private class DummyEmployee<A> {
    public A address;

    DummyEmployee(A a) {
      address = a;
    }
  }

  private class DummyAddress<B> {
    public B street;

    DummyAddress(B s) {
      street = s;
    }
  }

  public class DummyStruct {
    public int myInt;
    public Integer myInteger;
    public String myString;
    // Note: this is a recursive struct
    public DummyStruct dummyStruct;
    public List<String> myListString;
    public Map<String, String> myMapStringString;
    public DummyEmployee<DummyAddress<String>> employee;
    // Test arrays
    public int[] ints;
    // Test transient field
    public transient int t;
  }

  public class DummyParentStruct {
    public DummyInnerClass innerClass;
    public class DummyInnerClass {
      public int i;
    }
  }
}
