angular.module(PKG.name + '.commons')
  .controller('MySearchCtrl', function($state, mySettings, $alert) {
    this.searchTxt = '';
    this.mode = 'SEARCH';

    this.onSearch = function(event) {
      if (event.keyCode === 13) {
        switch(this.mode) {
          case 'SEARCH':
            $state.go('search.list');
            break;
          case 'PIN':
            mySettings.get('user-pins', true)
              .then(
                function(pins) {
                  if(!pins) {
                    pins = [];
                  }
                  pins.push({
                    label: event.target.value,
                    name: $state.current.name,
                    params: $state.params
                  });
                  return mySettings.set('user-pins', pins);
                }
              )
              .then(
                function success() {
                  $alert({
                    type: 'success',
                    content: 'Pin ' + event.target.value + ' saved successfully'
                  });
                },
                function error() {
                  $alert({
                    type: 'danger',
                    content: 'Pin ' + event.target.value + ' could not be saved'
                  });
                }
              );
        }
      }

    };
  });
