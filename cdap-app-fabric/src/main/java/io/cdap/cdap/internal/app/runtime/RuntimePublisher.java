package io.cdap.cdap.internal.app.runtime;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.monitor.MonitorMessage;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class RuntimePublisher {
    private static final Logger LOG = LoggerFactory.getLogger(RuntimePublisher.class);
    private static final Gson GSON = new Gson();
    private final CConfiguration cConf;
    private final MessagingContext messagingContext;
    private final Map<String, String> requestKeyToLocalTopic;
    private final RemoteExecutionLogProcessor logProcessor;

    RuntimePublisher(CConfiguration cConf, MessagingContext messagingContext, RemoteExecutionLogProcessor logProcessor) {
        this.cConf = cConf;
        this.messagingContext = messagingContext;
        this.requestKeyToLocalTopic = createTopicConfigs(cConf);
        this.logProcessor = logProcessor;
    }

    private static Map<String, String> createTopicConfigs(CConfiguration cConf) {
        return cConf.getTrimmedStringCollection(Constants.RuntimeMonitor.TOPICS_CONFIGS).stream().flatMap(key -> {
            int idx = key.lastIndexOf(':');
            if (idx < 0) {
                return Stream.of(Maps.immutableEntry(key, cConf.get(key)));
            }

            try {
                int totalTopicCount = Integer.parseInt(key.substring(idx + 1));
                if (totalTopicCount <= 0) {
                    throw new IllegalArgumentException("Total topic number must be positive for system topic config '" +
                            key + "'.");
                }
                // For metrics, We make an assumption that number of metrics topics on runtime are not different than
                // cdap system. So, we will add same number of topic configs as number of metrics topics so that we can
                // keep track of different offsets for each metrics topic.
                // TODO: CDAP-13303 - Handle different number of metrics topics between runtime and cdap system
                String topicPrefix = key.substring(0, idx);
                return IntStream
                        .range(0, totalTopicCount)
                        .mapToObj(i -> Maps.immutableEntry(topicPrefix + ":" + i, cConf.get(topicPrefix) + i));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Total topic number must be a positive number for system topic config'"
                        + key + "'.", e);
            }
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void publishMetadata(Map<String, Deque<MonitorMessage>> messageMap) throws Exception {
        long lastPublishTimestamp = 0;
        String lastPublishMessageId;
        for (Map.Entry<String, Deque<MonitorMessage>> entry : messageMap.entrySet()) {
            String topicConfig = entry.getKey();
            Deque<MonitorMessage> messages = entry.getValue();

            if (messages.isEmpty()) {
                continue;
            }

            System.out.println("wyzhang: topicConfig " + topicConfig);
            messages.forEach(m -> System.out.println("wyzhang: message" + m));

            String topic = requestKeyToLocalTopic.get(topicConfig);
            // publish messages to tms
            if (topic.startsWith(cConf.get(Constants.Logging.TMS_TOPIC_PREFIX))) {
                logProcessor.process(messages.stream().map(MonitorMessage::getMessage).iterator());
            } else {
                MessagePublisher messagePublisher = messagingContext.getMessagePublisher();
                messagePublisher.publish(NamespaceId.SYSTEM.getNamespace(), topic,
                        messages.stream().map(MonitorMessage::getMessage).iterator());
            }

            // Not persisting the ID of last message published. This might cause messages to be published more than once.
            // TODO(wyzhang): is the above okay?

            MonitorMessage lastMessage = messages.getLast();
            lastPublishMessageId = lastMessage.getMessageId();
            lastPublishTimestamp = getMessagePublishTime(lastMessage);
        }
    }

    private long getMessagePublishTime(MonitorMessage message) {
        return new MessageId(Bytes.fromHexString(message.getMessageId())).getPublishTimestamp();
    }
}
