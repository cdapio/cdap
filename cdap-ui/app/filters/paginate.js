angular.module(PKG.name+'.filters').filter('myPaginate', function() {
  return function (input, page, limit) {
    var pageLimit = limit || 10, // default value is 10 per page
        start = (page-1)*pageLimit,
        end = start + pageLimit;

        if (input.length < start) {
          start = 0;
          end = pageLimit;
        }

    return input.slice(start, end);
  };
});
