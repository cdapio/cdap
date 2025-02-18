/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.common.metrics;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.http.internal.HandlerInfo;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Map;
import org.junit.Test;

public class MetricsReporterHookTest {
    static final String TESTSERVICENAME = "test.Service";
    static final String TESTHANDLERNAME = "test.handler";
    static final String TESTMETHODNAME = "testMethod";

    @Test
    public void testReponseTimeCollection() throws InterruptedException {
        MetricsContext mockCollector = mock(MetricsContext.class);
        MetricsCollectionService mockCollectionService = mock(MetricsCollectionService.class);
        Map<String, String> tags = ImmutableMap.of(
            "ns", "system",
            "cmp", TESTSERVICENAME,
            "hnd", "handler",
            "mtd", TESTMETHODNAME
        );
        when(mockCollectionService.getContext(tags)).thenReturn(mockCollector);

        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://ignore");
        HandlerInfo handlerInfo = new HandlerInfo(TESTHANDLERNAME, TESTMETHODNAME);
        MetricsReporterHook hook = new MetricsReporterHook(CConfiguration.create(),
                                                           mockCollectionService, TESTSERVICENAME);

        hook.preCall(request, null, handlerInfo);
        hook.postCall(request, HttpResponseStatus.OK, handlerInfo);

        verify(mockCollector).event(eq("response.latency"), anyLong());
    }

    @Test
    public void testNamespacesExtraction() throws InterruptedException {
        MetricsContext mockCollector = mock(MetricsContext.class);
        MetricsCollectionService mockCollectionService = mock(MetricsCollectionService.class);
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
            "http://ignore/namespaces/testNamespace");
        HandlerInfo handlerInfo = new HandlerInfo(TESTHANDLERNAME, TESTMETHODNAME);
        MetricsReporterHook hook = new MetricsReporterHook(CConfiguration.create(),
            mockCollectionService, TESTSERVICENAME);

        Map<String, String> tags = ImmutableMap.of(
            "ns", "testNamespace",
            "cmp", TESTSERVICENAME,
            "hnd", "handler",
            "mtd", TESTMETHODNAME
        );
        when(mockCollectionService.getContext(tags)).thenReturn(mockCollector);

        hook.preCall(request, null, handlerInfo);
        verify(mockCollector).increment("request.received", 1);

        hook.postCall(request, HttpResponseStatus.OK, handlerInfo);
        verify(mockCollector).increment("response.successful", 1);
        verify(mockCollector).event(eq("response.latency"), anyLong());
    }

    @Test
    public void testNoNamespaceToExtract() throws InterruptedException {
        MetricsContext mockCollector = mock(MetricsContext.class);
        MetricsCollectionService mockCollectionService = mock(MetricsCollectionService.class);
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
            "http://ignore/namespaces");
        HandlerInfo handlerInfo = new HandlerInfo(TESTHANDLERNAME, TESTMETHODNAME);
        MetricsReporterHook hook = new MetricsReporterHook(CConfiguration.create(),
            mockCollectionService, TESTSERVICENAME);

        Map<String, String> tags = ImmutableMap.of(
            "ns", "system",
            "cmp", TESTSERVICENAME,
            "hnd", "handler",
            "mtd", TESTMETHODNAME
        );
        when(mockCollectionService.getContext(tags)).thenReturn(mockCollector);

        hook.preCall(request, null, handlerInfo);
        verify(mockCollector).increment("request.received", 1);

        hook.postCall(request, HttpResponseStatus.OK, handlerInfo);
        verify(mockCollector).increment("response.successful", 1);
        verify(mockCollector).event(eq("response.latency"), anyLong());
    }
}
