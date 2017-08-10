/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.test.messaging

import co.cask.cdap.api.data.DatasetContext
import co.cask.cdap.api.messaging.Message
import co.cask.cdap.api.messaging.MessageFetcher
import co.cask.cdap.api.spark.AbstractSpark
import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.api.spark.SparkMain
import com.google.common.base.Stopwatch
import org.apache.spark.SparkContext
import org.apache.tephra.TransactionFailureException
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
  *
  */
class MessagingSpark extends AbstractSpark with SparkMain {

  import MessagingSpark._

  override protected def configure(): Unit = setMainClass(getClass)

  /**
    * This method will be called when the Spark program starts.
    *
    * @param sec the implicit context for interacting with CDAP
    */
  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext
    val messagingContext = sec.getMessagingContext

    // Create a topic
    sec.getAdmin().createTopic(MessagingApp.TOPIC)

    // Publish a messaging without transaction
    val publisher = messagingContext.getMessagePublisher()
    publisher.publish(sec.getNamespace(), MessagingApp.TOPIC, "start")

    // Start an explicit transaction, publish a message, but then throw an exception so that the tx won't be committed
    try {
      Transaction(() => {
        LOG.info("In first Transaction block")
        val result = sc.parallelize(Seq(1, 2, 3, 4, 5)).reduce(_ + _)
        publisher.publish(sec.getNamespace, MessagingApp.TOPIC, "result-" + result)
        throw new Exception("Intentional")
      })

      // Shouldn't reach here
      throw new IllegalStateException("Expected TransactionFailureException");
    } catch {
      case t: TransactionFailureException => LOG.info("Exception expected: {}", t)
    }

    publisher.publish(sec.getNamespace(), MessagingApp.TOPIC, "block");

    // Wait for a control message to proceed
    fetchMessage(messagingContext.getMessageFetcher(), sec.getNamespace(), MessagingApp.CONTROL_TOPIC,
                 Option.empty, 5, TimeUnit.SECONDS);

    // Start another explicit transaction and publish a message
    Transaction((dc: DatasetContext) => {
      val result = sc.parallelize(Seq(1, 2, 3, 4, 5)).reduce(_ + _)
      publisher.publish(sec.getNamespace, MessagingApp.TOPIC, "result-" + result)
    })
  }

  private def fetchMessage(fetcher: MessageFetcher, namespace: String, topic: String,
                           afterMessageId: Option[String], timeout: Long, unit: TimeUnit): Message = {
    var iterator = fetcher.fetch(namespace, topic, 1, afterMessageId.orNull)
    val stopwatch = new Stopwatch().start
    try {
      while (!iterator.hasNext && stopwatch.elapsedTime(unit) < timeout) {
        TimeUnit.MILLISECONDS.sleep(100)
        iterator = fetcher.fetch(namespace, topic, 1, afterMessageId.orNull)
      }
      if (!iterator.hasNext) {
        throw new TimeoutException("Failed to get any messages from " + topic +
          " in " + timeout + " " + unit.name.toLowerCase)
      }
      // The payload contains the message to publish in next step
      return iterator.next
    } finally {
      iterator.close()
    }
  }
}

object MessagingSpark {
  val LOG = LoggerFactory.getLogger(classOf[MessagingSpark])
}
