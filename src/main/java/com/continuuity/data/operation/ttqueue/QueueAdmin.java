package com.continuuity.data.operation.ttqueue;

import com.continuuity.data.operation.ReadOperation;
import com.continuuity.data.operation.ttqueue.internal.EntryPointer;
import com.continuuity.data.operation.ttqueue.internal.GroupState;
import com.continuuity.hbase.ttqueue.HBQQueueMeta;
import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class QueueAdmin {

  /**
   * Generates and returns a unique group id for the speicified queue.
   */
  public static class GetGroupID extends ReadOperation {

    private final byte [] queueName;

    public GetGroupID(final byte [] queueName) {
      this.queueName = queueName;
    }

    public byte [] getQueueName() {
      return this.queueName;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("queuename", Bytes.toString(this.queueName))
          .toString();
    }
  }

  public static class GetQueueInfo extends ReadOperation {

    /** Unique id for the operation */
    private final byte [] queueName;

    public GetQueueInfo(byte[] queueName) {
      this.queueName = queueName;
    }

    public GetQueueInfo(final long id,
                        byte[] queueName) {
      super(id);
      this.queueName = queueName;
    }

    public byte [] getQueueName() {
      return this.queueName;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("queuename", Bytes.toString(this.queueName))
          .toString();
    }
  }

  public static class QueueInfo {
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
      if (other == this) return true;
      if (other instanceof QueueInfo) {
        return Objects.equal(this.jsonString, ((QueueInfo)other).jsonString);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return jsonString == null ? 0 : jsonString.hashCode();
    }

    public QueueInfo(HBQQueueMeta meta) {
      try {
        this.jsonString = meta.toJSON();
      } catch (JSONException e) {
        LOG.warn("Error converting HBQQueueMeta to JSON: " + e.getMessage());
        this.jsonString = new JSONObject().toString();
      }
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

  public static class QueueMeta {
    long globalHeadPointer;
    long currentWritePointer;
    GroupState [] groups;
    String jsonString = null;

    public long getGlobalHeadPointer() {
      return this.globalHeadPointer;
    }

    public long getCurrentWritePointer() {
      return this.currentWritePointer;
    }

    public GroupState [] getGroups() {
      return this.groups;
    }

    public String getJSONString() {
      return this.jsonString;
    }

    public QueueMeta() { }

    public QueueMeta(long globalHeadPointer, long currentWritePointer,
                     GroupState[] groups) {
      this.globalHeadPointer = globalHeadPointer;
      this.currentWritePointer = currentWritePointer;
      this.groups = groups;
    }

    public QueueMeta(HBQQueueMeta queueMeta) {
      try {
        this.jsonString = queueMeta.toJSON();
      } catch (JSONException e) {
        e.printStackTrace();
      }
    }

    public boolean isHBQMeta() {
      return this.jsonString != null;
    }

    public String toJSON() throws JSONException {
      if (jsonString != null)
        return jsonString;
      return getJSONObject().toString();
    }

    public JSONObject getJSONObject() throws JSONException {
      JSONObject outer = new JSONObject();
      outer.put("global", this.globalHeadPointer);
      outer.put("current", this.currentWritePointer);
      JSONArray groupArray = new JSONArray();
      if (this.groups != null && this.groups.length !=0 ) {
        for (GroupState group : this.groups) {
          JSONObject inner = new JSONObject();
          inner.put("groupsize", group.getGroupSize());
          inner.put("execmode", group.getMode().name());
          JSONObject innner = new JSONObject();
          EntryPointer head = group.getHead();
          innner.put("entryid", head.getEntryId());
          innner.put("shardid", head.getShardId());
          inner.put("head", innner);
          groupArray.put(inner);
        }
      }
      outer.put("groups", groupArray);
      return outer;
    }

    @Override
    public String toString() {
      if (this.jsonString != null) {
        return this.jsonString;
      }
      return Objects.toStringHelper(this)
          .add("globalHeadPointer", this.globalHeadPointer)
          .add("currentWritePointer", this.currentWritePointer)
          .add("groups", this.groups)
          .toString();
    }

    @Override
    public boolean equals(Object object) {
      if (object == null || !(object instanceof QueueMeta))
        return false;
      QueueMeta other = (QueueMeta)object;
      if (this.jsonString != null) {
        return this.jsonString.equals(other.jsonString);
      }
      return
          this.currentWritePointer == other.currentWritePointer &&
          this.globalHeadPointer == other.globalHeadPointer &&
          Arrays.equals(this.groups, other.groups);
    }
  }
}
