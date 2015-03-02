'use strict';

describe('myEllipsis', function() {

  beforeEach(module('cdap-ui.filters'));

  var ellipsis;
  var $filter;

  beforeEach(inject(function(_$filter_) {
    $filter = _$filter_;
  }));

  beforeEach(function() {
    ellipsis = $filter('myEllipsis');
  });

  it('should truncate string longer that limit to limit plus ellipsis', function() {

    var input = 'string longer than ten characters';
    var limit = 10;

    expect(ellipsis(input, limit).length).toBe(limit + 1);
    expect(ellipsis(input, limit)).toBe(input.substr(0, limit-1) + '\u2026 ');

  });

  it('should not truncate string with length less than limit', function() {

    var input = 'short';
    var limit = 10;

    expect(ellipsis(input, limit).length).toBe(input.length);
    expect(ellipsis(input, limit)).toEqual(input);

  });

  it('should return input when input is not type string', function() {

    var input = 12;
    expect(ellipsis(input, 10)).toEqual(input);

    input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    expect(ellipsis(input, 5)).toEqual(input);

    input = {
      1: 1,
      2: 2,
      3: 3,
      4: 4,
      5: 5
    };
    expect(ellipsis(input, 3)).toEqual(input);

  });

  it('should return null when input is null', function() {

    var input = null;
    expect(ellipsis(input, 10)).toEqual(null);

  });

});