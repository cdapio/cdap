# This is a chef role, to configure logstash

name "logstash_splunk_lite"
description "logstash agent configuration to enable streaming to splunk-lite"

# Here's where we create our variables

default_attributes(
  :logstash => {
    :join_groups => [ "continuuity" ],
    :patterns => {
      :continuuity => {
      }
    },
    :agent => {
      :inputs => [
        :file => {
          # Files are ${component}-continuuity-${fqdn}.log
          :path => [ '/var/log/continuuity/*.log' ]
        }
      ], # End of inputs
    } # End of agent
  } # End of logstash
)
