package io.cdap.cdap.internal.events;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.messaging.Message;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.subscriber.AbstractMessagingSubscriberService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;

public class EventSubscriber extends AbstractMessagingSubscriberService<Event<?>> {
    private static final Logger LOG = LoggerFactory.getLogger(EventSubscriber.class);
    //TODO Determinate events topic name for TMS
    private static final String TMS_EVENTS_TOPIC = "";
    private static final String CONSUMER_NAME = "events.writer";
    private final Gson GSON = new Gson();

    private final TransactionRunner transactionRunner;
    private final MultiThreadMessagingContext messagingContext;

    public EventSubscriber(CConfiguration cConf, MessagingService messagingService,
                           MetricsCollectionService metricsCollectionService,
                           TransactionRunner transactionRunner) {
        super(NamespaceId.SYSTEM.topic(TMS_EVENTS_TOPIC),
                cConf.getInt(Constants.Metadata.MESSAGING_FETCH_SIZE),
                cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT),
                cConf.getLong(Constants.Metadata.MESSAGING_POLL_DELAY_MILLIS),
                RetryStrategies.fromConfiguration(cConf, "system.events."),
                metricsCollectionService.getContext(
                        ImmutableMap.of(Constants.Metrics.Tag.COMPONENT, Constants.Service.PREVIEW_HTTP,
                                Constants.Metrics.Tag.INSTANCE_ID, "0",
                                Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
                                Constants.Metrics.Tag.TOPIC, TMS_EVENTS_TOPIC,
                                Constants.Metrics.Tag.CONSUMER, CONSUMER_NAME
                        )));

        this.transactionRunner = transactionRunner;
        this.messagingContext = new MultiThreadMessagingContext(messagingService);
    }

    @Override
    protected MessagingContext getMessagingContext() {
        return messagingContext;
    }

    @Override
    protected ProgramStatusEvent decodeMessage(Message message) throws Exception {
        return message.decodePayload(payload -> GSON.fromJson(payload, ProgramStatusEvent.class));
    }

    @Override
    protected TransactionRunner getTransactionRunner() {
        return transactionRunner;
    }

    @Nullable
    @Override
    protected String loadMessageId(StructuredTableContext context) throws Exception {
        AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
        return appMetadataStore.retrieveSubscriberState(getTopicId().getTopic(), CONSUMER_NAME);
    }

    @Override
    protected void storeMessageId(StructuredTableContext context, String messageId) throws Exception {
        AppMetadataStore appMetadataStore = AppMetadataStore.create(context);
        appMetadataStore.persistSubscriberState(getTopicId().getTopic(), CONSUMER_NAME, messageId);
    }

    @Override
    protected void processMessages(StructuredTableContext structuredTableContext, Iterator<ImmutablePair<String, Event<?>>> messages) throws Exception {
        while (messages.hasNext()) {
            ImmutablePair<String, Event<?>> next = messages.next();

            String messageId = next.getFirst();
            Event<?> event = next.getSecond();

            LOG.debug("Event with messageID " + messageId + " -> " + GSON.toJson(event));
        }
    }
}
