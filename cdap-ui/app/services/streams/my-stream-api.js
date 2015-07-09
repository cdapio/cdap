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
      create: myHelpers.getConfig('PUT', 'REQUEST', basepath),
      setProperties: myHelpers.getConfig('PUT', 'REQUEST', basepath + '/properties'),
      truncate: myHelpers.getConfig('POST', 'REQUEST', basepath + '/truncate'),
      programsList: myHelpers.getConfig('GET', 'REQUEST', basepath + '/programs', true),
      eventSearch: myHelpers.getConfig('GET', 'REQUEST', basepath + '/events', true),
      sendEvent: myHelpers.getConfig('POST', 'REQUEST', basepath, false, {json: false}),
      delete: myHelpers.getConfig('DELETE', 'REQUEST', basepath)
    });
  });
