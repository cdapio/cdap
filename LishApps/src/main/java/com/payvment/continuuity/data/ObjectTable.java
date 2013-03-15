package com.payvment.continuuity.data;

import com.continuuity.api.data.*;
import com.continuuity.api.data.dataset.table.*;
import com.google.gson.Gson;
import java.nio.charset.Charset;
import java.util.Map;
import com.google.common.base.Throwables;


import javax.annotation.Nullable;

public class ObjectTable extends DataSet {
    private final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {

        @Override
        protected Gson initialValue() {
           return new Gson();
        }
    };

     private final Table table;

    private void test() {
     ObjectTable table = new ObjectTable("a");
    table.put("x1", new Integer(1));
     table.put("x2", new String("1"));
     table.put("x3", new Float(1));
     Integer i = table.get("x2", Integer.class);
     }

     public ObjectTable(String name) {
     super(name);
     table = new Table("o_" + name);
     }
    public <T> void put(String key, T o) {
     String s = gson.get().toJson(o);
     try {
         this.table.write(new Write(key.getBytes(Charset.forName("UTF-8")),
        "default".getBytes(Charset.forName("UTF-8")),
          s.getBytes(Charset.forName("UTF-8"))));
           } catch (OperationException e) {
            throw Throwables.propagate(e);
            }
       }

    @Nullable
    public <T> T get(String key, Class<T> keyClass) {
        try {
            OperationResult<Map<byte[], byte[]>> result
                    = this.table.read(new Read(key.getBytes(Charset.forName("UTF-8")),
                    "default".getBytes(Charset.forName("UTF-8"))));
            if (result == null) {
                return null;
            }
            byte[] value = result.getValue().get(key);
            return (T) (gson.get().fromJson(new String(value), keyClass));
        } catch (OperationException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public DataSetSpecification configure() {
     return null;
     }
}