angular.module(PKG.name + '.feature.adapters', [])
  .constant('IMPLICIT_SCHEMA', {
    clf: '{"type":"record","name":"etlSchemaBody","fields":[{"name":"remote_host","type":["string","null"]},{"name":"remote_login","type":["string","null"]},{"name":"auth_user","type":["string","null"]},{"name":"date","type":["string","null"]},{"name":"request","type":["string","null"]},{"name":"status","type":["int","null"]},{"name":"content_length","type":["int","null"]},{"name":"referrer","type":["string","null"]},{"name":"user_agent","type":["string","null"]}]}',

    syslog: '{"type":"record","name":"etlSchemaBody","fields":[{"name":"timestamp","type":["string","null"]},{"name":"logsource","type":["string","null"]},{"name":"program","type":["string","null"]},{"name":"message","type":["string","null"]},{"name":"pid","type":["string","null"]}]}'
  });
