/**
 * Intended for truncating long namespace display name
 **/
angular.module(PKG.name+'.filters').filter('myEllipsis', function() {
  return function (input, limit) {
    return input.length > limit ? input.substr(0, limit-1) + '\u2026 ' : input;
  };
});
