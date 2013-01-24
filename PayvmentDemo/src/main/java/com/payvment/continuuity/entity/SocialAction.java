package com.payvment.continuuity.entity;

import com.continuuity.flow.flowlet.annotation.TupleSerializable;

public class SocialAction {

  public Long id;
  
  public Long date;
  
  public String type;
  
  @TupleSerializable
  public SocialActionType socialActionType;

  public Long product_id;
  
  public Long store_id;
  
  public String [] country;

  public String category;
  
  public Long actor_id;

  public SocialAction() {}
  
  public SocialAction(Long id, Long date, String type, Long product_id,
      Long store_id, String [] country, String category, Long actor_id) {
    this.id = id;
    this.date = date;
    this.type = type;
    this.product_id = product_id;
    this.store_id = store_id;
    this.country = country;
    this.category = category;
    this.actor_id = actor_id;
  }

  /**
   * Converts this product meta entry to JSON.
   * @return this product meta as a JSON string
   */
  public String toJson() {
    return
        "{\"@id\":\"" + this.id + "\"," +
        "\"type\":\"" + this.type + "\"," +
        "\"date\":\"" + this.date + "\"," +
        "\"product_id\":\"" + this.product_id + "\"," +
        "\"store_id\":\"" + this.store_id + "\"," +
        "\"country\":" + toJson(this.country) + "," +
        "\"category\":\"" + this.category + "\"," +
        "\"actor_id\":\"" + this.actor_id + "\"}";
  }
  
  public static String toJson(String [] country) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    boolean first = true;
    for (String c : country) {
      if (first) first = false;
      else sb.append(",");
      sb.append("\"");
      sb.append(c);
      sb.append("\"");
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SocialAction)) return false;
    SocialAction oSA = (SocialAction)o;
    if (id != oSA.id) return false;
    if (date != oSA.date) return false;
    if (!type.equals(oSA.type)) return false;
    if (product_id != oSA.product_id) return false;
    if (store_id != oSA.store_id) return false;
    if (actor_id != oSA.actor_id) return false;
    if (!category.equals(oSA.category)) return false;
    if (country.length != oSA.country.length) return false;
    for (int i=0; i<country.length; i++) {
      if (!country[i].equals(oSA.country[i])) return false;
    }
    return true;
  }
  
  public SocialActionType getSocialActionType() {
    if (this.socialActionType != null) return this.socialActionType;
    if (this.type == null) return null;
    return this.socialActionType = SocialActionType.fromString(this.type);
  }
  
  public static enum SocialActionType {
    ORDER, PINIT, COMMENT, LIKE, TWEET,
    YAY, MEH, NAY,
    OWN_ORDER, OWN_PINIT, OWN_COMMENT, OWN_LIKE,
    OWN_YAY, OWN_MEH, OWN_NAY;
    
    public static SocialActionType fromString(String type) {
      if (type == null) return null;
      
      if (type.equals("order-action")) return ORDER;
      if (type.equals("pinit-action")) return PINIT;
      if (type.equals("exp-comment-action")) return COMMENT;
      if (type.equals("like-action")) return LIKE;
      if (type.equals("tweet-action")) return TWEET;
      if (type.equals("yay-exp-action")) return YAY;
      if (type.equals("meh-exp-action")) return MEH;
      if (type.equals("nay-exp-action")) return NAY;
      
      if (type.equals("own-order-action")) return OWN_ORDER;
      if (type.equals("own-pinit-action")) return OWN_PINIT;
      if (type.equals("own-exp-comment-action")) return OWN_COMMENT;
      if (type.equals("own-like-action")) return OWN_LIKE;
      if (type.equals("own-yay-exp-action")) return OWN_YAY;
      if (type.equals("own-meh-exp-action")) return OWN_MEH;
      if (type.equals("own-nay-exp-action")) return OWN_NAY;
      
      throw new IllegalArgumentException("Unknown social action type: " + type);
    }

    public long getScore() {
      switch (this) {
        case ORDER:   return 6L;
        case PINIT:   return 3L;
        case COMMENT: return 4L;
        case LIKE:
        case TWEET:   return 4L;
        case YAY:     return 2L;
        case MEH:     return 1L;
        case NAY:     return 0L;
        default:      return 0L;
      }
    }
  }
}
