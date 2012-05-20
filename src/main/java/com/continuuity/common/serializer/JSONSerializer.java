package com.continuuity.common.serializer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * Serializes and Deserializes data as and from JSON.
 * <p>
 *   This implementation uses Gson (Google JSON) serializer and
 *   deserializer. It operates on object of type T.
 * </p>
 */
public class JSONSerializer<T> {
  /**
   * Instance of gson for serializing and deserializing instance of T.
   */
  public final transient Gson gson;
  {
    gson = new Gson();
  }

  /**
   * Raised when there is issue with serialization or deserialization.
   * The message explains why.
   */
  @SuppressWarnings("serial")
  public static class JSONSerializationException extends RuntimeException {
    public JSONSerializationException(String msg) {
      super(msg);
    }
  }

  /**
   * Serializes the object of type T into JSON and returns a byte array representing JSON.
   * @param object instance of type T
   * @return byte array of JSON for object of type T.
   * @throws JSONSerializationException
   */
  public final byte[] serialize(T object) throws JSONSerializationException {
    try {
      String json = gson.toJson((T) object);
      return json.getBytes();
    } catch (Exception e) {
      throw new JSONSerializationException(e.getMessage());
    }
  }

  /**
   * Deserializes the byte array of JSON into the object of type T
   * @param object instance of type T
   * @param clazz specifying the type of object T
   * @return Object instance of type T
   * @throws JSONSerializationException
   */
  public final <T> T deserialize(byte[] object, Class<T> clazz) throws JSONSerializationException {
    try {
      T o = gson.fromJson(new String(object), clazz);
      return o;
    } catch (JsonSyntaxException e) {
      throw new JSONSerializationException(e.getMessage());
    }
  }
}
