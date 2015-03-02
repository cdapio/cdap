'use strict';

describe('myEllipsis', function() {

  beforeEach(module('cdap-ui.filters'));

  var ellipsis;

  beforeEach(inject(function(_$filter_) {
    ellipsis = _$filter_('myEllipsis');

  }));

  beforeEach(function() {
  });

  it('should truncate string longer that limit to limit plus ellipsis', function() {

    var input = 'string longer than ten characters';
    var limit = 10;

    var result = ellipsis(input, limit);

    expect(result.length).toBe(limit + 1);
    expect(result).toBe(input.substr(0, limit-1) + '\u2026 ');

  });

  it('should not truncate string with length less than limit', function() {

    var input = 'short';
    var limit = 10;

    var result = ellipsis(input, limit);

    expect(result.length).toBe(input.length);
    expect(result).toEqual(input);

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