angular.module(PKG.name + '.feature.services')
  .controller('StatusMakeRequestController', function($scope, $state, MyDataSource) {
    $scope.programId = $state.params.programId;
    $scope.requestUrl = $state.params.requestUrl;
    $scope.requestMethod = $state.params.requestMethod;
    $scope.urlParams = [];
    $scope.queryParams = [];
    $scope.response = null;
    $scope.postBody = {};

    var pattern = /\{([\w\-]+)\}/g,
        dataSrc = new MyDataSource($scope);

    $scope.requestUrl.split('?')
      .forEach(function(item, index) {
        var patternMatch;
        if (index === 0) {
          // url params
          item.split('/')
            .forEach(function(item) {
              if (item.length === 0) { return;}
              patternMatch = item.match(pattern);
              $scope.urlParams.push({
                /* If the url param matches the pattern {param1}
                    then add that as key,
                    otherwise mark key as null

                    (/count/{word} vs /count/stats/)
                    In the former case '{word}' will be added as a key and
                    will be recognised in the template to be replaced with
                    usertyped value where as in the latter example null will
                    be added to key and will be just displayed in the template.
                 */
                key: ( angular.isArray(patternMatch) ? patternMatch: null),
                value: item
              });
            });
        } else {
          // query params
          item.split('&')
            .forEach(function(item) {
              var pat,
                  keyValArr;
              if (item.length === 0) { return;}
              pat = item.match(pattern);
              // Could be dynamic or static query params.
              // 1. /count/{word}?filter={filterType} vs
              // 2. /count/{word}?filter={filterType}&aggregate=10
              // In the latter case we still need to include the query
              // param 'aggregate' to be a static value
              if (angular.isArray(pat)) {
                $scope.queryParams.push({
                  key: pat[0].substr(1, (pat[0].length - 2)),
                  value: pat[0]
                });
              } else {
                keyValArr = item.split('=');
                $scope.queryParams.push({
                  key: keyValArr[0],
                  value: keyValArr[1]
                });
              }
            });
        }
      });

    $scope.makeRequest = function() {
      var compiledUrl = '/apps/' +
        $state.params.appId + '/services/' +
        $state.params.programId + '/methods';

      angular.forEach($scope.urlParams, function(param) {
        compiledUrl = compiledUrl + '/' + param.value;
      });

      angular.forEach($scope.queryParams, function(param, index) {
        compiledUrl += (index === 0 ? '?': '&') +
                        param.key + '=' + encodeURIComponent(param.value);
      });

      var requestObj = {
        _cdapNsPath: compiledUrl,
        method: $scope.requestMethod.toUpperCase()
      };

      if ($scope.requestMethod === 'POST' || $scope.requestMethod === 'PUT') {
        angular.extend(requestObj, {
          body: $scope.postBody
        });
      }

      dataSrc.request(requestObj)
        .then(function(res) {
          $scope.response = res;
        });
    };

    $scope.$watch('queryParams', resetResponse, true);
    $scope.$watch('urlParams', resetResponse, true);

    function resetResponse() {
      $scope.response = null;
    }


  });
