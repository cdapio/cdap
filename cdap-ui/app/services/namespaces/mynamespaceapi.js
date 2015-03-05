angular.module(PKG.name + '.services')
  .factory('myNamespaceApi', function(MySocketProvider, $state, myCdapUrl) {
    var url = myCdapUrl.constructUrl;
    
    return MySocketProvider(
      url({
        _cdapPath: '/namespaces/:namespaceId'
      }),
      {
        namespaceId: '@namespaceId'
      },
      {
        create: {
          method: 'PUT'
        }
      }

    )
  });
