// adapted from https://gist.github.com/thomseddon/3511330

angular.module(PKG.name+'.filters').filter('bytes', function() {
  return function(bytes, precision) {
    if (bytes<1 || isNaN(parseFloat(bytes)) || !isFinite(bytes)) {
      return '0 bytes';
    }
    if (typeof precision === 'undefined') {
      precision = bytes>1023 ? 1 : 0;
    }
    var number = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) +
      ' ' + ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'][number];
  }
});