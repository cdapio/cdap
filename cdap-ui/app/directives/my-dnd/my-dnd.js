angular.module(PKG.name + '.commons')
  .directive('draggable', function() {
    return function(scope, element, attrs) {
      var el = element[0];
      var dropZone = attrs.dropZone,
          data = attrs.model;
       el.draggable = true;

       el.addEventListener(
           'dragstart',
           function(e) {
               e.dataTransfer.effectAllowed = 'copy';
               e.dataTransfer.setData('Id', this.id);
               e.dataTransfer.setData('DropZone', dropZone);
               e.dataTransfer.setData('Data', data);
               this.classList.add('drag');
               return false;
           },
           false
       );

       el.addEventListener(
           'dragend',
           function(e) {
               this.classList.remove('drag');
               return false;
           },
           false
       );
    }
  })

  .directive('droppable', function() {
    return {
        scope: {
            drop: '&' // parent
        },
        link: function(scope, element) {
          var el = element[0];

          el.addEventListener(
            'dragover',
            function(e) {
                e.dataTransfer.dropEffect = 'copy';
                // allows us to drop
                if (e.preventDefault) e.preventDefault();
                this.classList.add('over');
                return false;
            },
            false
          );

          el.addEventListener(
            'dragenter',
            function(e) {
                this.classList.add('over');
                return false;
            },
            false
          );

          el.addEventListener(
            'dragleave',
            function(e) {
                this.classList.remove('over');
                return false;
            },
            false
          );

            el.addEventListener(
                'drop',
                function(e) {
                    // Stops some browsers from redirecting.
                    if (e.stopPropagation) e.stopPropagation();

                    this.classList.remove('over');
                    var id = e.dataTransfer.getData('Id'),
                        dropZone = e.dataTransfer.getData('DropZone'),
                        data = e.dataTransfer.getData('Data');

                    // var item = document.getElementById(e.dataTransfer.getData('Text'));
                    // this.appendChild(item);

                    // call the drop passed drop function
                    scope.$apply(function(scope) {
                        var fn = scope.drop();
                        if ('undefined' !== typeof fn) {
                          fn(id, dropZone, data);
                        }
                    });
                    return false;
                },
                false
            );
        }
    }
  });
