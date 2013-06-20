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

import com.continuuity.api.common.Bytes;

/**
 * Stores all data for a product feed event, or can be a holder of the metadata
 * of a single product.
 */
public class ProductFeedEntry {

  public Long productId;
  public Long storeId;
  public Long date;
  public String[] country;
  public String category;
  public String name;
  public Double score;

  public ProductFeedEntry() {
  }

  public ProductFeedEntry(Long productId,
                          Long storeId,
                          Long date,
                          String[] country,
                          String category,
                          String name,
                          Double score) {

    this.productId = productId;
    this.storeId = storeId;
    this.date = date;
    this.country = country;
    this.category = category;
    this.name = name;
    this.score = score;
  }

  //@Override
  public byte[] toBytes() {
    int len = 3 * Bytes.SIZEOF_LONG + Bytes.SIZEOF_DOUBLE +
      3 * Bytes.SIZEOF_INT + this.category.length() + this.name.length();

    for (String c : country) {
      len += Bytes.SIZEOF_INT + c.length();
    }

    byte[] bytes = new byte[len];
    int idx = 0;
    idx = Bytes.putLong(bytes, idx, this.productId);
    idx = Bytes.putLong(bytes, idx, this.storeId);
    idx = Bytes.putLong(bytes, idx, this.date);
    idx = Bytes.putDouble(bytes, idx, this.score);
    idx = Bytes.putInt(bytes, idx, this.country.length);
    for (String c : country) {
      idx = Bytes.putInt(bytes, idx, c.length());
      idx = Bytes.putBytes(bytes, idx, Bytes.toBytes(c), 0, c.length());
    }
    idx = Bytes.putInt(bytes, idx, this.category.length());
    idx = Bytes.putBytes(bytes, idx,
                         Bytes.toBytes(this.category), 0, this.category.length());
    idx = Bytes.putInt(bytes, idx, this.name.length());
    idx = Bytes.putBytes(bytes, idx,
                         Bytes.toBytes(this.name), 0, this.name.length());
    assert (idx == len);
    return bytes;
  }

  //@Override
  public ProductFeedEntry fromBytes(byte[] bytes) {
    int idx = 0;
    this.productId = Bytes.toLong(bytes, idx);
    idx += Bytes.SIZEOF_LONG;
    this.storeId = Bytes.toLong(bytes, idx);
    idx += Bytes.SIZEOF_LONG;
    this.date = Bytes.toLong(bytes, idx);
    idx += Bytes.SIZEOF_LONG;
    this.score = Bytes.toDouble(bytes, idx);
    idx += Bytes.SIZEOF_DOUBLE;
    int numCountries = Bytes.toInt(bytes, idx);
    idx += Bytes.SIZEOF_INT;
    this.country = new String[numCountries];
    for (int i = 0; i < numCountries; i++) {
      int len = Bytes.toInt(bytes, idx);
      idx += Bytes.SIZEOF_INT;
      this.country[i] = Bytes.toString(bytes, idx, len);
      idx += len;
    }
    int len = Bytes.toInt(bytes, idx);
    idx += Bytes.SIZEOF_INT;
    this.category = Bytes.toString(bytes, idx, len);
    idx += len;
    len = Bytes.toInt(bytes, idx);
    idx += Bytes.SIZEOF_INT;
    this.name = Bytes.toString(bytes, idx, len);
    idx += len;
    return this;
  }

  /**
   * Converts this product meta entry to JSON.
   *
   * @return this product meta as a JSON string
   */
  public String toJson() {
    return
      "{\"@id\":\"" + this.productId + "\"," +
        "\"country\":" + SocialAction.toJson(country) + "," +
        "\"category\":\"" + this.category + "\"," +
        "\"name\":\"" + this.name + "\"," +
        "\"last_modified\":\"" + this.date + "\"," +
        "\"storeId\":\"" + this.storeId + "\"," +
        "\"score\":\"" + this.score + "\"}";
  }
}
