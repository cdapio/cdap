#!/bin/sh

: ${APP_HOME?"not set."}

echo "Status for all apps, all flows..."
$APP_HOME/bin/reactor-client status --application AggregateMetrics  --flow AggMetricsByTag
$APP_HOME/bin/reactor-client status --application AggregateMetrics  --flow RandomMetrics
$APP_HOME/bin/reactor-client status --application CountAndFilterWords  --flow CountAndFilterWords
$APP_HOME/bin/reactor-client status --application CountCounts --flow CountCounts
$APP_HOME/bin/reactor-client status --application CountOddAndEven --flow CountOddAndEven
$APP_HOME/bin/reactor-client status --application CountRandom --flow CountRandom
$APP_HOME/bin/reactor-client status --application CountTokens --flow CountTokens
$APP_HOME/bin/reactor-client status --application HelloWorld --flow whoFlow
$APP_HOME/bin/reactor-client status --application SimpleWriteAndRead --flow SimpleWriteAndRead
$APP_HOME/bin/reactor-client status --application WordCount --flow WordCounter
echo "Done."

