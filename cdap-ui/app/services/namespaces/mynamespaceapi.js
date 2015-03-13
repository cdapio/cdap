angular.module(PKG.name + '.services')
  .factory('myNamespaceApi', function($resource, $state, myCdapUrl, myAuth) {
    var url = myCdapUrl.constructUrl;

    return $resource(
      url({
        _cdapPath: '/namespaces/:namespaceId'
      }),
      {
        namespaceId: '@namespaceId'
      },
      {
        create: {
          method: 'PUT',
          options: { type: 'REQUEST' },
          user: myAuth.currentUser || null,
          headers: {
            authorization: (myAuth.currentUser.token ? 'Bearer ' + myAuth.currentUser.token: null)
          },
          interceptor: {
            response: function(response) {
              return response;
            }
          }
        }
      }

    )
  });
