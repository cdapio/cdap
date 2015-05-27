angular.module(PKG.name + '.services')
  .factory('myStreamApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        listPath = '/namespaces/:namespace/streams',
        basepath = '/namespaces/:namespace/streams/:streamId';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      streamId: '@streamId'
    },
    {
      list: myHelpers.getConfig('GET', 'REQUEST', listPath, true),
      get: myHelpers.getConfig('GET', 'REQUEST', basepath),
      create: myHelpers.getConfig('PUT', 'REQUEST', basepath)
    });
  });
