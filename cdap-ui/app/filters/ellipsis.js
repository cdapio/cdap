/**
 * Intended for truncating long namespace display name
 **/
angular.module(PKG.name+'.filters').filter('myEllipsis', function() {
  return function (input, limit) {
    if (typeof input === 'string') {
      return input.length > limit ? input.substr(0, limit-1) + '\u2026 ' : input;
    } else {
      return input;
    }
  };
});
