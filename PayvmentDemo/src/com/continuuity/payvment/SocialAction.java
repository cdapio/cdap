package com.continuuity.payvment;

public class SocialAction {

  public Long id;
  
  public Long date;
  
  public String type;
  
  public SocialActionType socialActionType;

  public Long product_id;
  
  public Long actor_id;

  public SocialActionType getSocialActionType() {
    if (this.socialActionType != null) return this.socialActionType;
    if (this.type == null) return null;
    return this.socialActionType = SocialActionType.fromString(this.type);
  }
  
  public static enum SocialActionType {
    ORDER, PINIT, COMMENT, LIKE,
    YAY, MEH, NAY,
    OWN_ORDER, OWN_PINIT, OWN_COMMENT, OWN_LIKE,
    OWN_YAY, OWN_MEH, OWN_NAY;
    
    public static SocialActionType fromString(String type) {
      if (type == null) return null;
      
      if (type.equals("order-action")) return ORDER;
      if (type.equals("pinit-action")) return PINIT;
      if (type.equals("exp-comment-action")) return COMMENT;
      if (type.equals("like-action")) return LIKE;
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
        case ORDER:   return 5L;
        case PINIT:   return 3L;
        case COMMENT: return 3L;
        case YAY:     return 2L;
        case MEH:     return 1L;
        case NAY:     return 0L;
        default:      return 0L;
      }
    }
  }
}
