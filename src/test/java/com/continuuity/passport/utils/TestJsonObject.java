package com.continuuity.passport.utils;

import com.continuuity.passport.http.handlers.Utils;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestJsonObject {

  @Test
  public void testJsonUtils() {
    String s = Utils.getJsonError("Error");
    assertTrue(s != null);
    assertTrue(s.equals("{\"error\":\"Error\"}"));

    String auth = Utils.getAuthenticatedJson("foo");
    assertTrue(auth != null);
    assertTrue(auth.equals("{\"error\":null,\"result\":\"foo\"}"));

    String nonce = Utils.getIdJson(null, "10");
    assertTrue(nonce != null);
    assertTrue(nonce.equals("{\"error\":null,\"result\":\"10\"}"));

    String ok = Utils.getJsonOK();
    assertTrue(ok != null);
    assertTrue(ok.equals("{\"error\":null}"));

  }


  @Test
  public void testJsonSerialization() {

    Gson gson = new Gson();
    String s = "{\"password\":\"foo\"}";
    JsonObject o = gson.fromJson(s, JsonElement.class).getAsJsonObject();
    String str = o.get("password").getAsString();
    assertTrue("foo".equals(str));
    //String str = gson.fromJson(s, String.class);

  }

}
