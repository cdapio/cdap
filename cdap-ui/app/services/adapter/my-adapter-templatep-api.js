angular.module(PKG.name + '.services')
  .factory('myAdapterTemplatesApi', function($resource) {
    return $resource(
      '',
      {
        apptype: '@apptype',
        appname: '@appname'
      },
      {
        query: {
          url:'/predefinedapps/:apptype',
          method: 'GET',
          isArray: true
        },
        get: {
          url: '/predefinedapps/:apptype/:appname',
          method: 'GET'
        }
      }
    );
  });
