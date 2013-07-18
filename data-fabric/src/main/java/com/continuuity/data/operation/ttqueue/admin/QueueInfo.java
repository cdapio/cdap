package com.continuuity.data.operation.ttqueue.admin;

import com.google.common.base.Objects;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
*
*/
public class QueueInfo {
  String jsonString;

  private static final Logger LOG =
      LoggerFactory.getLogger(QueueInfo.class);

  public String getJSONString() {
    return this.jsonString;
  }

  @Override
  public String toString() {
    return jsonString == null ? "<null>" : jsonString;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof QueueInfo) {
      return Objects.equal(this.jsonString, ((QueueInfo) other).jsonString);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return jsonString == null ? 0 : jsonString.hashCode();
  }

  public QueueInfo(QueueMeta meta) {
    try {
      this.jsonString = meta.toJSON();
    } catch (JSONException e) {
      LOG.warn("Error converting QueueMeta to JSON: " + e.getMessage());
      this.jsonString = new JSONObject().toString();
    }
  }

  public QueueInfo(String json) {
    this.jsonString = json;
  }
}
