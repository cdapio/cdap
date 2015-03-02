/**
 * myTitleFilter
 * intended for use in the <title> tag.
 */

angular.module(PKG.name+'.filters').filter('myTitleFilter',
function myTitleFilter () {

  return function(state) {
    var title = state.data && state.data.title;
    return (title ? title + ' | ' : '') + 'CDAP';
  };

});