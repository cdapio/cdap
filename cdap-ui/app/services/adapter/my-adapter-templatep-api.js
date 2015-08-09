angular.module(PKG.name + '.services')
  .factory('myAdapterTemplatesApi', function($resource) {
    return $resource(
      '',
      {
        templatetype: '@templatetype',
        templatename: '@templatename'
      },
      {
        query: {
          url:'/adaptertemplates/:templatetype',
          method: 'GET',
          isArray: true
        },
        get: {
          url: '/adaptertemplates/:templatetype/:templatename',
          method: 'GET'
        }
      }
    );
  });
