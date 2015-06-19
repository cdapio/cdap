package co.cask.cdap.template.test;

import co.cask.cdap.api.data.schema.Schema;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by rohannahata on 6/18/15.
 */
public class Temp {
  @Test
  public void tempTest() throws IOException {
    String schemaStr = "{\"type\":\"record\",\"name\":\"streamEvent\",\"fields\":[{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\",\"keys\":\"string\"}},{\"name\":\"ticker\",\"type\":\"string\"},{\"name\":\"num\",\"type\":\"int\"},{\"name\":\"price\",\"type\":\"double\"}]}";
    Schema.parseJson(schemaStr);
  }
}
