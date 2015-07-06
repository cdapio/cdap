angular.module(PKG.name + '.services')
  .factory('myExploreApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        basepath = '/namespaces/:namespace/data/explore/tables',
        querypathNs = '/namespaces/:namespace/data/explore/queries',
        querypath = '/data/explore/queries/:queryhandle';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      table: '@table',
      queryhandle: '@queryhandle'
    },
    {
      list: myHelpers.getConfig('GET', 'REQUEST', basepath, true),
      getInfo: myHelpers.getConfig('GET', 'REQUEST', basepath + '/:table/info'),
      postQuery: myHelpers.getConfig('POST', 'REQUEST', querypathNs),
      getQueries: myHelpers.getConfig('GET', 'REQUEST', querypathNs, true),
      getQuerySchema: myHelpers.getConfig('GET', 'REQUEST', querypath + '/schema', true),
      getQueryPreview: myHelpers.getConfig('POST', 'REQUEST', querypath + '/preview', true)
    });
  });
