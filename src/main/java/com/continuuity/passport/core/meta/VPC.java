package com.continuuity.passport.core.meta;

/**
 *
 */
public class VPC {

  private int vpcId ;

  private String vpcName;

  public VPC(String vpcName) {
    this(-1,vpcName);
  }
  public VPC(int vpcId, String vpcName) {
    this.vpcId = vpcId;
    this.vpcName = vpcName;
  }

  public int getVpcId() {
    return vpcId;
  }

  public String getVpcName() {
    return vpcName;
  }
}
