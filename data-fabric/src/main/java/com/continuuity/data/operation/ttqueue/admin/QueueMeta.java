package com.continuuity.data.operation.ttqueue.admin;

import com.continuuity.data.operation.ttqueue.internal.EntryPointer;
import com.continuuity.data.operation.ttqueue.internal.GroupState;
import com.continuuity.hbase.ttqueue.HBQQueueMeta;
import com.google.common.base.Objects;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Arrays;

/**
*
*/
public class QueueMeta {
  long globalHeadPointer;
  long currentWritePointer;
  GroupState[] groups;
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

  public void setGlobalHeadPointer(long globalHeadPointer) {
    this.globalHeadPointer = globalHeadPointer;
  }

  public void setCurrentWritePointer(long currentWritePointer) {
    this.currentWritePointer = currentWritePointer;
  }

  public void setGroups(GroupState[] groups) {
    this.groups = groups;
  }

  public QueueMeta() { }

  public QueueMeta(long globalHeadPointer, long currentWritePointer, GroupState[] groups) {
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
