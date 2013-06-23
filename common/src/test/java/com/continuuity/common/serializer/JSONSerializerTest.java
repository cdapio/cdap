package com.continuuity.common.serializer;

import com.google.common.base.Objects;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class JSONSerializerTest {

  public static class MySimpleclass {
    private int i;
    private int j;
    private String s;
    private Integer k;
    private Long l;
    private List<String> ls = new ArrayList<String>();

    public MySimpleclass(int i, int j) {
      this.i = i;
      this.j = j;
      this.s = new String("continuuity");
      this.k = new Integer(99);
      this.l = new Long(23131L);
      ls.add("CAFEDOOD");
      ls.add("CMAGIC");
      ls.add("STRIP_MAGIC");
      ls.add("LSOMACGIC");
      ls.add("WANPIPE_MAGIC");
    }

    @Override
    public boolean equals(Object other) {
      if(other == null) return false;
      MySimpleclass that = (MySimpleclass) other;
      return Objects.equal(i, that.i) &&
        Objects.equal(j, that.j) &&
        Objects.equal(s, that.s) &&
        Objects.equal(k, that.k) &&
        Objects.equal(l, that.l) &&
        Objects.equal(ls, that.ls);
    }
  }

  final MySimpleclass m = new MySimpleclass(9, 10);

  @Test
  public void testSerialize() throws Exception {
    JSONSerializer<MySimpleclass> serializer = new JSONSerializer<MySimpleclass>();
    byte[] b = serializer.serialize(m);
    Assert.assertNotNull(b);
  }

  @Test
  public void testDeserialize() throws Exception {
    JSONSerializer<MySimpleclass> serializer = new JSONSerializer<MySimpleclass>();
    byte[] b = serializer.serialize(m);
    MySimpleclass h = serializer.deserialize(b, MySimpleclass.class);
    Assert.assertTrue(h.equals(m));
  }

}
