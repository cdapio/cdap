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



  public VPC(String vpcName, String vpcLabel, String vpcType) {
    this(-1, vpcName, vpcLabel,-1,vpcType);
  }

  public VPC(int vpcId, String vpcName, String vpcLabel,String vpcType) {
    this(vpcId,vpcName,vpcLabel,-1,vpcType);
  }

  public VPC(int vpcId, String vpcName, String vpcLabel, long createdTime) {
    this(vpcId,vpcName,vpcLabel,createdTime,"");
  }

  public VPC(int vpcId, String vpcName, String vpcLabel, long createdTime, String vpcType) {
    this.vpcId = vpcId;
    this.vpcName = vpcName;
    this.vpcLabel = vpcLabel;
    this.createdTime = createdTime;
    this.vpcType = vpcType;
  }


  public int getVpcId() {
    return vpcId;
  }

  public String getVpcLabel() {
    return vpcLabel;
  }

  public String getVpcName() {
    return vpcName;
  }

  public String getVpcType() {
    return vpcType;
  }

  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.toJson(this);
  }

  public static VPC fromString(String jsonString) {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.fromJson(jsonString, VPC.class);
  }

}
