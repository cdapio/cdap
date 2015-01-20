angular.module(PKG.name + '.commons')
  .directive('myMetricPicker', function (MyDataSource) {

    var dSrc = new MyDataSource();

    function MetricPickerCtrl ($scope) {

      var types = ['system', 'user'];

      $scope.available = {
        types: types,
        contexts: [],
        metrics: []
      };

      $scope.metric = {
        type: types[0],
        context: '',
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

        function fetchAhead () {
          var val = (scope.metric.context||'').replace(/\.$/, ''),
              url = ['/metrics', scope.metric.type, val].join('/');

          dSrc.request(
            {
              _cdapNsPath: url + '?search=childContext'
            },
            function (result) {
              scope.available.contexts = result.map(function (child) {
                return val ? val + '.' + child : child;
              });
            }
          );

          if(val) {
            // assigning the promise directly works
            // only because results are used as is.
            scope.available.metrics = dSrc.request(
              {
                _cdapNsPath: url + '/metrics'
              }
            );
          }
          else {
            scope.available.metrics = [];
          }

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
            var parts = [
              '/metrics',
              newVal.type,
            ];
            if(newVal.context) {
              parts.push(newVal.context);
            }
            parts.push(newVal.name);

            ngModel.$setViewValue(parts.join('/'));
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
