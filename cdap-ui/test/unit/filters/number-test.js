describe('myNumber', function() {

  beforeEach(module('cdap-ui.filters'));

  var number;

  beforeEach(inject(function(_$filter_) {
    number = _$filter_('myNumber');

  }));

  it('should not convert number < 1000 ', function() {
    var b = 10;

    expect(number(b)).toBe('10');
  });

  it('should convert to k ', function() {
    var b = 1000;

    expect(number(b)).toBe('1k');
  });

  it('should convert to M ', function() {
    var b = 1000000;

    expect(number(b)).toBe('1.0M');
  });

  it('should convert to G ', function() {
    var b = 1000000000;

    expect(number(b)).toBe('1.0G');
  });

  it('should convert to T ', function() {
    var b = 1000000000000;

    expect(number(b)).toBe('1.0T');
  });

  it('should convert to P ', function() {
    var b = 1000000000000000;

    expect(number(b)).toBe('1.0P');
  });

});