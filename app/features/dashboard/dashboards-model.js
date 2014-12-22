/**
 * Dashboards' model
 */

angular.module(PKG.name+'.feature.dashboard').factory('myDashboardsModel',
function ($q, Widget) {


  function Dashboard (title, colCount) {
    var c = [[]];
    for (var i = 1; i < (colCount||3); i++) {
      c.push([]);
    };

    c[0].push(new Widget());
    this.title = title || 'Dashboard';
    this.columns = c;
  }


  /**
   * rename a widget
   */
  Dashboard.prototype.renameWidget = function (widget, newName) {
    if(newName) {
      widget.title = newName;
    }
  };


  /**
   * remove a widget from the active dashboard tab
   * @param  {object} w the widget object
   */
  Dashboard.prototype.removeWidget = function (widget) {
    var that = this;
    angular.forEach(that.columns, function (c, i) {
      that.columns[i] = c.filter(function (p) {
        return widget !== p;
      });
    });
  };



  /**
   * add a widget to the active dashboard tab
   */
  Dashboard.prototype.addWidget = function (w) {
    var c = this.columns,
        index = 0,
        smallest;

    // find the column with the least widgets
    for (var i = 0; i < c.length; i++) {
      var len = c[i].length;
      if(smallest===undefined || (len < smallest)) {
        smallest = len;
        index = i;
      }
    }

    this.columns[index].unshift(w || new Widget({title: 'just added'}));
  };



  /**
   * rename dashboard tab
   */
  Dashboard.prototype.rename = function (newName) {
    if(newName) {
      this.title = newName;
    }
  };


/* ------------------------------------------------------------------------- */


  function Model () {
    this.data = [
      new Dashboard('grid'),
      new Dashboard('full-width', 1)
    ];

    this.data.activeIndex = 0;
  }


  /**
   * direct access to the current tab's Dashboard instance
   */
  Model.prototype.current = function () {
    return this.data[this.data.activeIndex || 0];
  };



  /**
   * remove a dashboard tab
   */
  Model.prototype.remove = function (index) {
    this.data.splice(index, 1);
  };


  /**
   * add a new dashboard tab
   */
  Model.prototype.add = function (title, colCount) {
    var n = this.data.push(new Dashboard(title, colCount));
    this.data.activeIndex = n-1;
  };



  return new Model();
});


