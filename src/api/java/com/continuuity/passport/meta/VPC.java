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

  public VPC(String vpcName, String vpcLabel) {
    this(-1, vpcName, vpcLabel);
  }

  public VPC(int vpcId, String vpcName, String vpcLabel) {
    this.vpcId = vpcId;
    this.vpcName = vpcName;
    this.vpcLabel = vpcLabel;
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

  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    return gson.toJson(this);
  }
}
