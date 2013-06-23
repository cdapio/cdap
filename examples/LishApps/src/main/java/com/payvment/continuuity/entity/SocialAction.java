/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.payvment.continuuity.entity;

/**
 *
 */
public class SocialAction {

  public Long id;
  public Long date;
  public String type;
  public SocialActionType socialActionType;
  public Long productId;
  public Long storeId;
  public String[] country;
  public String category;
  public Long actorId;

  public SocialAction() {
  }

  public SocialAction(Long id, Long date, String type, Long productId,
                      Long storeId, String[] country, String category, Long actorId) {
    this.id = id;
    this.date = date;
    this.type = type;
    this.productId = productId;
    this.storeId = storeId;
    this.country = country;
    this.category = category;
    this.actorId = actorId;
  }

  /**
   * Converts this product meta entry to JSON.
   *
   * @return this product meta as a JSON string
   */
  public String toJson() {
    return
      "{\"@id\":\"" + this.id + "\"," +
        "\"type\":\"" + this.type + "\"," +
        "\"date\":\"" + this.date + "\"," +
        "\"productId\":\"" + this.productId + "\"," +
        "\"storeId\":\"" + this.storeId + "\"," +
        "\"country\":" + toJson(this.country) + "," +
        "\"category\":\"" + this.category + "\"," +
        "\"actorId\":\"" + this.actorId + "\"}";
  }

  public static String toJson(String[] country) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    boolean first = true;
    for (String c : country) {

      if (first) {
        first = false;
      } else {
        sb.append(",");
      }

      sb.append("\"");
      sb.append(c);
      sb.append("\"");
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof SocialAction)) {
      return false;
    }

    SocialAction oSA = (SocialAction) o;

    if (id != oSA.id) {
      return false;
    }
    if (date != oSA.date) {
      return false;
    }
    if (!type.equals(oSA.type)) {
      return false;
    }
    if (productId != oSA.productId) {
      return false;
    }
    if (storeId != oSA.storeId) {
      return false;
    }
    if (actorId != oSA.actorId) {
      return false;
    }
    if (!category.equals(oSA.category)) {
      return false;
    }
    if (country.length != oSA.country.length) {
      return false;
    }

    for (int i = 0; i < country.length; i++) {
      if (!country[i].equals(oSA.country[i])) {
        return false;
      }
    }

    return true;
  }

  public SocialActionType getSocialActionType() {
    if (this.socialActionType != null) {
      return this.socialActionType;
    }

    if (this.type == null) {
      return null;
    }

    return this.socialActionType = SocialActionType.fromString(this.type);
  }

  /**
   *
   */
  public static enum SocialActionType {
    ORDER, PINIT, COMMENT, LIKE, TWEET,
    YAY, MEH, NAY,
    OWN_ORDER, OWN_PINIT, OWN_COMMENT, OWN_LIKE,
    OWN_YAY, OWN_MEH, OWN_NAY;

    public static SocialActionType fromString(String type) {

      if (type == null) {
        return null;
      }
      if (type.equals("order-action")) {
        return ORDER;
      }
      if (type.equals("pinit-action")) {
        return PINIT;
      }
      if (type.equals("exp-comment-action")) {
        return COMMENT;
      }
      if (type.equals("like-action")) {
        return LIKE;
      }
      if (type.equals("tweet-action")) {
        return TWEET;
      }
      if (type.equals("yay-exp-action")) {
        return YAY;
      }
      if (type.equals("meh-exp-action")) {
        return MEH;
      }
      if (type.equals("nay-exp-action")) {
        return NAY;
      }
      if (type.equals("own-order-action")) {
        return OWN_ORDER;
      }
      if (type.equals("own-pinit-action")) {
        return OWN_PINIT;
      }
      if (type.equals("own-exp-comment-action")) {
        return OWN_COMMENT;
      }
      if (type.equals("own-like-action")) {
        return OWN_LIKE;
      }
      if (type.equals("own-yay-exp-action")) {
        return OWN_YAY;
      }
      if (type.equals("own-meh-exp-action")) {
        return OWN_MEH;
      }
      if (type.equals("own-nay-exp-action")) {
        return OWN_NAY;
      }

      throw new IllegalArgumentException("Unknown social action type: " + type);
    }

    public long getScore() {
      switch (this) {
        case ORDER:
          return 6L;
        case PINIT:
          return 3L;
        case COMMENT:
          return 4L;
        case LIKE:
        case TWEET:
          return 4L;
        case YAY:
          return 2L;
        case MEH:
          return 1L;
        case NAY:
          return 0L;
        default:
          return 0L;
      }
    }
  }
}
