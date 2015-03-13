describe('CDAP', function() {
  it('should have CDAP as browser title', function() {
    browser.get('http://localhost:8080');
    element(by.css('a[ui-sref="login"]')).click();
    expect(browser.getTitle()).toEqual('CDAP');
  });


  describe('Management', function() {
    it('should highlight the Management tab', function() {
      // navigate to admin console
      element(by.css('a[ui-sref="admin.overview"]')).click();
      expect(element(by.css('li[ng-class="{active: highlightTab == \'management\'}"')).getAttribute('class'))
        .toMatch('active');
    });

    var sidebars = element.all(by.css('li[class="list-group-item"]'));

    it('should have 3 sidebars items in Admin', function() {

      expect(sidebars.count()).toEqual(3);

      expect(sidebars.get(0).getText()).toEqual('Management Home');
      expect(sidebars.get(1).getText()).toEqual('System Configuration');
      expect(sidebars.get(2).getText()).toEqual('Namespaces');
    });

    it('should highlight Management Home by default', function() {

      expect(sidebars.get(0).element(by.css('a')).getAttribute('class')).toMatch('current');
      expect(element.all(by.css('[class="current"]')).count()).toEqual(1);
    });

    describe('System Configuration > Services', function() {
      it('should show subnavigation when System Configuration is clicked', function() {
        sidebars.get(1).click();
        var submenu = sidebars.get(1).element(by.css('li'));
        expect(submenu.element(by.css('a')).getText()).toEqual('Services');
      });
      var services = element(by.css('a[ui-sref="admin.system.services"]'));
      it('should navigate to Services page when clicked', function() {
        services.click();
        expect(element(by.css('h3')).getText()).toEqual('Services');
      });

      it('should highlight services', function() {
        expect(services.getAttribute('class')).toMatch('current');
        expect(element.all(by.css('[class="current"]')).count()).toEqual(1);
      });
    });

    describe('Namespaces', function() {
      it('should show subnavigation when Namespaces is clicked', function() {
        element(by.linkText('Namespaces')).click();
        expect(element(by.linkText('Create Namespace'))).toBeDefined();
      });

      it('should navigate to Create Namespaces when clicked', function() {
        element(by.linkText('Create Namespace')).click();
        expect(element(by.css('h3')).getText()).toEqual('Add Namespace');
      });

      it('should highlight only Create Namespace', function(){
        var current = element.all(by.css('[class="current"]'));
        expect(current.count()).toEqual(1);
        expect(current.get(0).getText()).toEqual('Create Namespace');
      });

      it('should expand default namespace when clicked', function() {
        element(by.linkText('default')).click();
        expect(element(by.linkText('Apps'))).toBeDefined();
      });

      it('should navigate to Apps when clicked', function() {
        element(by.linkText('Apps')).click();
        expect(element(by.css('h2')).getText()).toEqual('default: Applications');
      });

      it('should highlight Apps', function() {
        var current = element.all(by.css('[class="current"]'));
        expect(current.count()).toEqual(1);
        expect(current.get(0).getText()).toEqual('Apps');
      });
    });

  });

});
