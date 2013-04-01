package com.continuuity.performance.util.json;

/**
 *
 */

import org.json.simple.JSONAware;
import org.json.simple.JSONStreamAware;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A JSON object. Key value pairs are unordered. JSONOrderedObject supports java.util.Map interface.
 *
 * @author FangYidong<fangyidong@yahoo.com.cn>
 */
public class JSONOrderedObject extends LinkedHashMap implements Map, JSONAware, JSONStreamAware {
  private static final long serialVersionUID = -503443796854799292L;

  /**
   * Encode a map into JSON text and write it to out.
   * If this map is also a JSONAware or JSONStreamAware, JSONAware or JSONStreamAware specific behaviours will be ignored at this top level.
   *
   * @see org.json.simple.JSONValue#writeJSONString(Object, Writer)
   *
   * @param map
   * @param out
   */
  public static void writeJSONString(Map map, Writer out) throws IOException {
    if(map == null){
      out.write("null");
      return;
    }

    boolean first = true;
    Iterator iter=map.entrySet().iterator();

    out.write('{');
    while(iter.hasNext()){
      if(first)
        first = false;
      else
        out.write(',');
      Map.Entry entry=(Map.Entry)iter.next();
      out.write('\"');
      out.write(escape(String.valueOf(entry.getKey())));
      out.write('\"');
      out.write(':');
      JSONValue.writeJSONString(entry.getValue(), out);
    }
    out.write('}');
  }

  public void writeJSONString(Writer out) throws IOException{
    writeJSONString(this, out);
  }

  /**
   * Convert a map to JSON text. The result is a JSON object.
   * If this map is also a JSONAware, JSONAware specific behaviours will be omitted at this top level.
   *
   * @see org.json.simple.JSONValue#toJSONString(Object)
   *
   * @param map
   * @return JSON text, or "null" if map is null.
   */
  public static String toJSONString(Map map){
    if(map == null)
      return "null";

    StringBuffer sb = new StringBuffer();
    boolean first = true;
    Iterator iter=map.entrySet().iterator();

    sb.append('{');
    while(iter.hasNext()){
      if(first)
        first = false;
      else
        sb.append(',');

      Map.Entry entry=(Map.Entry)iter.next();
      toJSONString(String.valueOf(entry.getKey()),entry.getValue(), sb);
    }
    sb.append('}');
    return sb.toString();
  }

  public String toJSONString(){
    return toJSONString(this);
  }

  private static String toJSONString(String key,Object value, StringBuffer sb){
    sb.append('\"');
    if(key == null)
      sb.append("null");
    else
      escapeFromJSonValue(key, sb);
    sb.append('\"').append(':');

    sb.append(JSONValue.toJSONString(value));

    return sb.toString();
  }

  public String toString(){
    return toJSONString();
  }

  public static String toString(String key,Object value){
    StringBuffer sb = new StringBuffer();
    toJSONString(key, value, sb);
    return sb.toString();
  }

  /**
   * Escape quotes, \, /, \r, \n, \b, \f, \t and other control characters (U+0000 through U+001F).
   * It's the same as JSONValue.escape() only for compatibility here.
   *
   * @see org.json.simple.JSONValue#escape(String)
   *
   * @param s
   * @return
   */
  public static String escape(String s){
    return JSONValue.escape(s);
  }
  static void escapeFromJSonValue(String s, StringBuffer sb) {
    for(int i=0;i<s.length();i++){
      char ch=s.charAt(i);
      switch(ch){
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\f':
          sb.append("\\f");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '/':
          sb.append("\\/");
          break;
        default:
          //Reference: http://www.unicode.org/versions/Unicode5.1.0/
          if((ch>='\u0000' && ch<='\u001F') || (ch>='\u007F' && ch<='\u009F') || (ch>='\u2000' && ch<='\u20FF')){
            String ss=Integer.toHexString(ch);
            sb.append("\\u");
            for(int k=0;k<4-ss.length();k++){
              sb.append('0');
            }
            sb.append(ss.toUpperCase());
          }
          else{
            sb.append(ch);
          }
      }
    }//for
  }
}
