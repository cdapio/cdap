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


import com.continuuity.test.AppFabricTestBase;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.StreamWriter;
import com.continuuity.testsuite.purchaseanalytics.PurchaseAnalyticsApp;
import com.google.common.reflect.TypeToken;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Word Count main test.
 */
public class TestPurchaseAnalytics extends AppFabricTestBase {

  static Type stringMapType = new TypeToken<Map<String, String>>() {
  }.getType();
  static Type objectMapType = new TypeToken<Map<String, Object>>() {
  }.getType();

  @Test
  public void TestDataSetUsage() throws IOException, TimeoutException, InterruptedException {
    ApplicationManager appManager = deployApplication(PurchaseAnalyticsApp.class);

    FlowManager purchaseAnalyticsFlow = appManager.startFlow("PurchaseAnalyticsFlow");

    StreamWriter s1 = appManager.getStreamWriter("transactionStream");
    s1.send("1|{\"customer\":\"alex\",\"product\":\"FisherPrice\",\"quantity\":10,\"price\":\"100\",\"purchaseTime\":\"129308132\"}");

  }
}
