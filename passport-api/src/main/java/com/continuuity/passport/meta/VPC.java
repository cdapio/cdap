/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.meta;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * TODO: Add a API getJSON() that returns JSONObject instead of toString override.
 */
public class VPC {

  private final int vpcId;

  private final String vpcName;

  private final String vpcLabel;

  private final long createdTime;

  private final String vpcType;

  /**
   * Construct VPC using vpcName, vpc label and vpc Type.
   * @param vpcName  VPC Name
   * @param vpcLabel VPC label
   * @param vpcType  VPC type
   */
  public VPC(String vpcName, String vpcLabel, String vpcType) {
    this(-1, vpcName, vpcLabel, -1, vpcType);
  }

  /**
   * Construct VPC using vpcName, vpc label and vpc Type.
   * @param vpcId   VPC ID
   * @param vpcName  VPC name
   * @param vpcLabel VPC label
   * @param vpcType  VPC type
   */
  public VPC(int vpcId, String vpcName, String vpcLabel, String vpcType) {
    this(vpcId, vpcName, vpcLabel, -1, vpcType);
  }

  /**
   * Construct VPC using vpcName, vpc label and vpc Type.
   * @param vpcId    VPC ID
   * @param vpcName  VPC name
   * @param vpcLabel VPC label
   * @param createdTime Vpc created time
   * @param vpcType  VPC type
   */
  public VPC(int vpcId, String vpcName, String vpcLabel, long createdTime, String vpcType) {
    this.vpcId = vpcId;
    this.vpcName = vpcName;
    this.vpcLabel = vpcLabel;
    this.createdTime = createdTime;
    this.vpcType = vpcType;
  }

  /**
   * Get ID of the VPC.
   * @return VPC ID
   */
  public int getVpcId() {
    return vpcId;
  }

  /**
   * Get VPC label of vpc.
   * @return  VPC label
   */
  public String getVpcLabel() {
    return vpcLabel;
  }

  /**
   * Get VPC name of the vpc.
   * @return VPC name
   */
  public String getVpcName() {
    return vpcName;
  }

  /**
   * Get VPC type.
   * @return vpcType
   */
  public String getVpcType() {
    return vpcType;
  }

  /**
   * Serialize VPC into a json serialized string.
   * @return String representing vpc - the field names will be lowercase with underscores seperating camel cased names
   */
  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.toJson(this);
  }

  /**
   * Deserialize VPC from json string.
   * @param jsonString String representing VPC
   * @return instance of {@code VPC}
   */
  public static VPC fromString(String jsonString) {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.fromJson(jsonString, VPC.class);
  }

}
