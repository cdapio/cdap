package com.continuuity.passport.core.meta;

/**
 *
 */
public class VPC {

  private final int vpc_id ;

  private final String vpc_name;

  public VPC(String vpcName) {
    this(-1,vpcName);
  }
  public VPC(int vpcId, String vpcName) {
    this.vpc_id = vpcId;
    this.vpc_name = vpcName;
  }

  public int getVpcId() {
    return vpc_id;
  }

  public String getVpcName() {
    return vpc_name;
  }
}
