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

    System.out.println("Clearing AppFabric...");
    clearAppFabric();
  }

  /**
   * Asserts that the specified product feed entries contain all the same
   * field values.
   *
   * @param productEntry1
   * @param productEntry2
   */
  public static void assertEqual(ProductFeedEntry productEntry1,
                                 ProductFeedEntry productEntry2) {
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
  public static void assertEqual(SocialAction socialAction1,
                                 SocialAction socialAction2) {
    assertEquals(socialAction1.id, socialAction2.id);
    assertEquals(socialAction1.product_id, socialAction2.product_id);
    assertEquals(socialAction1.store_id, socialAction2.store_id);
    assertEquals(socialAction1.actor_id, socialAction2.actor_id);
    assertEquals(socialAction1.date, socialAction2.date);
    assertEquals(socialAction1.category, socialAction2.category);
    assertEquals(socialAction1.type, socialAction2.type);
  }
}
