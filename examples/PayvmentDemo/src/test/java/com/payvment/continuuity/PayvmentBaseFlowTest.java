package com.payvment.continuuity;

import com.continuuity.test.AppFabricTestBase;
import com.payvment.continuuity.entity.ProductFeedEntry;
import com.payvment.continuuity.entity.SocialAction;
import org.junit.Before;

import static org.junit.Assert.assertEquals;

/**
 * Base class for running Payvment Flow tests.
 */
public class PayvmentBaseFlowTest extends AppFabricTestBase {

  @Before
  public void clearFabricBeforeEachTest() throws Exception {
    clearDataFabric();
  }

  /**
   * Asserts that the specified product feed entries contain all the same
   * field values.
   *
   * @param productEntry1
   * @param productEntry2
   */
  public static void assertEqual(ProductFeedEntry productEntry1, ProductFeedEntry productEntry2) {
    assertEquals(productEntry1.product_id, productEntry2.product_id);
    assertEquals(productEntry1.store_id, productEntry2.store_id);
    assertEquals(productEntry1.date, productEntry2.date);
    assertEquals(productEntry1.category, productEntry2.category);
    assertEquals(productEntry1.name, productEntry2.name);
    assertEquals(productEntry1.score, productEntry2.score);
  }

  /**
   * Asserts that the specified social actions contain all the same
   * field values.
   *
   * @param socialAction1
   * @param socialAction1
   */
  public static void assertEqual(SocialAction socialAction1, SocialAction socialAction2) {
    assertEquals(socialAction1.id, socialAction2.id);
    assertEquals(socialAction1.product_id, socialAction2.product_id);
    assertEquals(socialAction1.store_id, socialAction2.store_id);
    assertEquals(socialAction1.actor_id, socialAction2.actor_id);
    assertEquals(socialAction1.date, socialAction2.date);
    assertEquals(socialAction1.category, socialAction2.category);
    assertEquals(socialAction1.type, socialAction2.type);
  }
}
