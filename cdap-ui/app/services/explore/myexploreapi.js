angular.module(PKG.name + '.services')
  .factory('myExploreApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        basepath = '/namespaces/:namespace/data/explore/tables';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      table: '@table',
    },
    {
      list: myHelpers.getConfig('GET', 'REQUEST', basepath, true),
      getInfo: myHelpers.getConfig('GET', 'REQUEST', basepath + '/:table/info')
    });
  });
