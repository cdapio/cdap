package com.payvment.continuuity.entity;


import com.continuuity.api.data.lib.SimpleSerializable;
import com.continuuity.api.data.util.Bytes;

/**
 * Stores all data for a product feed event, or can be a holder of the metadata
 * of a single product.
 */
public class ProductFeedEntry implements SimpleSerializable {

  public Long product_id;

  public Long store_id;

  public Long date;

  public String category;

  public String name;

  public Double score;

  public ProductFeedEntry() {}

  public ProductFeedEntry(Long product_id, Long store_id, Long date,
      String category, String name, Double score) {
    this.product_id = product_id;
    this.store_id = store_id;
    this.date = date;
    this.category = category;
    this.name = name;
    this.score = score;
  }

  @Override
  public byte [] toBytes() {
    int len = 3 * Bytes.SIZEOF_LONG + Bytes.SIZEOF_DOUBLE +
        2 * Bytes.SIZEOF_INT + this.category.length() + this.name.length();
    byte [] bytes = new byte[len];
    int idx = 0;
    idx = Bytes.putLong(bytes, idx, this.product_id);
    idx = Bytes.putLong(bytes, idx, this.store_id);
    idx = Bytes.putLong(bytes, idx, this.date);
    idx = Bytes.putDouble(bytes, idx, this.score);
    idx = Bytes.putInt(bytes, idx, this.category.length());
    idx = Bytes.putBytes(bytes, idx,
        Bytes.toBytes(this.category), 0, this.category.length());
    idx = Bytes.putInt(bytes, idx, this.name.length());
    idx = Bytes.putBytes(bytes, idx,
        Bytes.toBytes(this.name), 0, this.name.length());
    assert(idx == len);
    return bytes;
  }

  @Override
  public ProductFeedEntry fromBytes(byte [] bytes) {
    int idx = 0;
    this.product_id = Bytes.toLong(bytes, idx); idx += Bytes.SIZEOF_LONG;
    this.store_id = Bytes.toLong(bytes, idx); idx += Bytes.SIZEOF_LONG;
    this.date = Bytes.toLong(bytes, idx); idx += Bytes.SIZEOF_LONG;
    this.score = Bytes.toDouble(bytes, idx); idx += Bytes.SIZEOF_DOUBLE;
    int len = Bytes.toInt(bytes, idx); idx += Bytes.SIZEOF_INT;
    this.category = Bytes.toString(bytes, idx, len); idx += len;
    len = Bytes.toInt(bytes, idx); idx += Bytes.SIZEOF_INT;
    this.name = Bytes.toString(bytes, idx, len); idx += len;
    return this;
  }

  /**
   * Converts this product meta entry to JSON.
   * @return this product meta as a JSON string
   */
  public String toJson() {
    return
        "{\"@id\":\"" + this.product_id + "\"," +
        "\"category\":\"" + this.category + "\"," +
        "\"name\":\"" + this.name + "\"," +
        "\"last_modified\":\"" + this.date + "\"," +
        "\"store_id\":\"" + this.store_id + "\"," +
        "\"score\":\"" + this.score + "\"}";
  }
}
