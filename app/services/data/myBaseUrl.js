angular.module(PKG.name + '.services')
  .factory('myBaseUrl', function myBaseUrl(MY_CONFIG) {
    return [
      'http://',
      MY_CONFIG.cdap.routerServerUrl,
      ':',
      MY_CONFIG.cdap.routerServerPort,
      '/v3'
    ].join('');
  });
