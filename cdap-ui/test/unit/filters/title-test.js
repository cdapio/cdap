describe('myTitleFilter', function() {

  beforeEach(module('cdap-ui.filters'));

  var title,
      $filter;

  beforeEach(inject(function(_$filter_) {
    $filter = _$filter_;
  }));

  beforeEach(function() {
    title = $filter('myTitleFilter');
  });

  it('should append CDAP to title', function() {
    var input = {
      data: {
        title: 'test'
      }
    };

    expect(title(input)).toBe(input.data.title + ' | CDAP');
  });

  it('should return CDAP when title is not present', function() {
    var input = {};

    expect(title(input)).toBe('CDAP');
  });

});