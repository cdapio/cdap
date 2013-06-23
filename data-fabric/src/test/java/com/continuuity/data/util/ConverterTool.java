package com.continuuity.data.util;

import com.continuuity.common.io.BinaryDecoder;
import com.continuuity.common.io.BinaryEncoder;
import com.continuuity.common.io.Decoder;
import com.continuuity.common.io.Encoder;
import com.google.common.collect.Maps;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ConverterTool {
  public static byte[] toBytes(Map<String,String> map) {
    byte[] mapAsBytes;
    if (map == null)
      return null;
    else {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Encoder encoder = new BinaryEncoder(bos);
      try {
        encoder.writeInt(map.size());
        for(Map.Entry<String,String> entry: map.entrySet()) {
          encoder.writeString(entry.getKey());
          encoder.writeString(entry.getValue());
        }
        mapAsBytes=bos.toByteArray();
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
      return mapAsBytes;
    }
  }

  public static Map<String,String> toMap(byte[] mapAsBytes) {
    Map<String,String> map=null;
    if (mapAsBytes == null) return map;
    else {
      ByteArrayInputStream bis = new ByteArrayInputStream(mapAsBytes);
      Decoder decoder = new BinaryDecoder(bis);
      int size;
      try {
        size = decoder.readInt();
        if (size>0) {
          map= Maps.newHashMap();
          for(int i=0; i<size; i++) {
            map.put(decoder.readString(),decoder.readString());
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
      return map;
    }
  }



}
