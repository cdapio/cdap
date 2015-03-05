angular.module(PKG.name + '.commons')
  .directive('myMetricPicker', function (MyDataSource, $stateParams, $log) {

    var dSrc = new MyDataSource();

    function MetricPickerCtrl ($scope) {

      var ns = [$stateParams.namespace,'namespace'].join(' ');

      $scope.available = {
        contexts: [],
        types: ['system', ns],
        names: []
      };

      $scope.metric = {
        context: '',
        type: ns,
        name: ''
      };

    }

    return {
      restrict: 'E',

      require: 'ngModel',

      scope: {},

      templateUrl: 'metric-picker/metric-picker.html',

      controller: MetricPickerCtrl,

      link: function (scope, elem, attr, ngModel) {

        if(attr.required!==undefined) {
          elem.find('input').attr('required', true);
        }

        function getBaseContext () {
          var output;

          if(scope.metric.type==='system') {
            output = 'system';
          }
          else {
            output = $stateParams.namespace;

            if(!output) { // should never happen, except on directive playground
              output = 'default';
              $log.warn('metric-picker using default namespace as context!');
            }

          }

          return 'namespace.' + output;
        }

        function fetchAhead () {
          var b = getBaseContext(),
              bLen = b.length+1,
              context = b;

          if(scope.metric.context) {
            context += '.' + scope.metric.context;
          }

          scope.available.contexts = [];
          dSrc.request(
            {
              method: 'POST',
              _cdapPath: '/metrics/search?target=childContext' +
                '&context=' + encodeURIComponent(context)
            },
            function (res) {
              scope.available.contexts = res.map(function(d){
                return {
                  value: d.substring(bLen),
                  display: d.substring(bLen+scope.metric.context.length)
                };
              }).filter(function(d) {
                return d.display;
              });
            }
          );

          scope.available.names = [];
          dSrc.request(
            {
              method: 'POST',
              _cdapPath: '/metrics/search?target=metric' +
                '&context=' + encodeURIComponent(context)
            },
            function (res) {
              scope.available.names = res;
            }
          );

        }

        var onBlurHandler = ngModel.$setTouched.bind(ngModel);
        elem.find('button').on('blur', onBlurHandler);
        elem.find('input').on('blur', onBlurHandler);

        scope.$watchCollection('metric', function (newVal, oldVal) {

          ngModel.$validate();

          if(newVal.type !== oldVal.type) {
            scope.metric.context = '';
            scope.metric.name = '';
            fetchAhead();
            return;
          }

          if(newVal.context !== oldVal.context) {
            scope.metric.name = '';
            fetchAhead();
          }

          if(newVal.context && newVal.name) {
            ngModel.$setViewValue({
              context: getBaseContext() + '.' + newVal.context,
              name: newVal.name
            });
          }
          else {
            if(ngModel.$dirty) {
              ngModel.$setViewValue(null);
            }

          }

        });

        fetchAhead();
      }
    };
  });
