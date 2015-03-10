angular.module(PKG.name+'.filters').filter('myNumber', function() {
  return function (input, precision) {
    if (input<1 || isNaN(parseFloat(input)) || !isFinite(input)) {
      return '0';
    }

    if (typeof precision === 'undefined') {
      precision = input > 1023 ? 1 : 0;
    }

    var number = Math.floor(Math.log(input) / Math.log(1000));
    return (input / Math.pow(1000, Math.floor(number))).toFixed(precision) +
      '' + ['', 'k', 'M', 'G', 'T', 'P'][number];

  };
});
