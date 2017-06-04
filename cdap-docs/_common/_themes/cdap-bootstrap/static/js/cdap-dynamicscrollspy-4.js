/**
 *  CDAP DynamicScrollSpy
 *  ---------------------
 *
 *  Based on AutoScrollspy v 0.1.0
 *  https://github.com/psalmody/dynamic-scrollspy
 *
 *  Revised by Cask Data, Inc.
 *
 */
(function($) {

    $.fn.DynamicScrollSpy = function(opts) {
        // Define options
        opts = (typeof(opts) == 'undefined') ? {} : opts;
        this.isinit = (typeof(this.isinit) == 'undefined') ? false : self.isinit;

        // Destroy scrollspy option
        if (opts == 'destroy') {
            this.isinit = false;
            this.empty();
            this.off('activate.bs.scrollspy');
            $('body').removeAttr('data-spy');
            return this;
        }

        // Extend options priorities: passed, existing, defaults
        this.options = $.extend({}, {
            // Use affix by default
            affix: true,
            // Offset for affix: use if an integer else calculate
            affixOffset: 0,
            // Lowest-level header to be included (H2)
            tH: 2,
            // Highest-level header to be included (H6)
            bH: 6,
            // jQuery filter
            exclude: false,
            // Generate random IDs?
            genIDs: false,
            // Offset for scrollspy
            offset: 100,
            // Add these class names to the top-most UL
            ulClassNames: '',
            // If testing, show heading tagName and ID
            testing: false,
            // Starting object to build the list from
            startingID: ''
        }, this.options, opts);

        var self = this;

        // Store used random numbers
        this.rands = [];

        // Encode any text in header title to HTML entities

        function encodeHTML(value) {
            return $('<div></div>').text(value).html();
        }

        // Returns jQuery object of all headers between tH and bH, optionally starting at startingID

        function selectAllH() {
            var st = [];
            for (var i = self.options.tH; i <= self.options.bH; i++) {
                st.push('H' + i);
            }
            if (self.options.startingID === '') {
                return $(st.join(',')).not(self.options.exclude);
            } else {
                return $(self.options.startingID).find(st.join(',')).not(self.options.exclude);
            }
        }

        // Generate random numbers; save and check saved to keep them unique

        function randID() {
            var r;

            function rand() {
                r = Math.floor(Math.random() * (1000 - 100)) + 100;
            }
            // Get first random number
            rand();
            while (self.rands.indexOf(r) >= 0) {
                // when that random is found, try again until it isn't
                rand();
            }
            // Save random for later
            self.rands.push(r);
            return r;
        }

        // Generate random IDs for elements if requested

        function genIDs() {
            selectAllH().prop('id', function() {
                // If no id prop for this header, return a random id
                return ($(this).prop('id') === '') ? $(this).prop('tagName') + (randID()) : $(this).prop('id');
            });
        }

        // Check that all have id attribute

        function checkIDs() {
            var missing = 0;
            var msg = '';
            // Check they exist first
            selectAllH().each(function() {
                if ($(this).prop('id') === '') {
                    missing++;
                } else {
                    if ($('[id="' + $(this).prop('id') + '"]').length > 1) {
                        msg = "DynamicScrollSpy: Error! Duplicate id " + $(this).prop('id');
                        throw new Error(msg);
                    }
                }
            });
            if (missing > 0) {
                msg = "DynamicScrollSpy: Not all headers have ids and genIDs: false.";
                throw new Error(msg);
            }
            return missing;
        }

        // Testing - show IDs and tag types

        function showTesting() {
            console.log('showTesting');
            selectAllH().append(function() {
                // Let's see the tag names (for testing)
                return ' (' + $(this).prop('tagName') + ', ' + $(this).prop('id') + ')';
            });
        }

        // Builds the list

        function makeList() {
            var selectAllHs = selectAllH();
            if (selectAllHs.length == 0) {
                return;
            } else {
                var currentLevel = self.options.tH;
                var inList = false;
                var ul = '<h3 class="nav dynamic-scrollspy-heading scrollspy-active"><a href="#" rel="nofollow">Contents</a></h3>';
                ul += '<ul class="nav dynamic-scrollspy ' + self.options.ulClassNames + '">';
                selectAllHs.each(function() {
                    var k = $(this).prop('id');
                    var dstext = encodeHTML($(this).text());
                    var lvl = Number($(this).prop('tagName').replace('H', ''));
                    var li = '<li id="dsli' + k + '" class="nav dynamic-scrollspy-title"><a href="#' + k + '">' + dstext + '</a>';
                    if (lvl < currentLevel) { // End current(s) and start new
                        ul += '</li></ul>'.repeat(currentLevel - lvl) + li;
                    } else if (lvl === currentLevel) {
                        if (inList) {
                            ul += '</li>' + li;
                        } else {
                            ul += li;
                            inList = true;
                        }
                    } else { // lvl > currentLevel ; should always go up in increments
                        ul += '<ul class="nav child dynamic-scrollspy-list" >' + li;
                    }
                    currentLevel = lvl;
                });
                ul += '</li></ul>';
                self.append($(ul));
            }
        }

        // Initialize plugin

        function init() {
            // First time (or after destroy)
            if (self.isinit === false) {
                // Generate IDs
                if (self.options.genIDs) {
                    genIDs();
                } else {
                    checkIDs();
                }
                // Show if testing
                if (self.options.testing) showTesting();

                makeList();

                if (self.options.affix) {
                    var ul = self.children('ul');
                    if (typeof self.options.affixOffset == 'number') {
                        self.children('ul').affix({
                            offset: {
                                top: self.options.affixOffset
                            }
                        });
                    } else {
                        self.children('ul').affix({
                            offset: {
                                top: function() {
                                    var c = ul.offset().top,
                                        d = parseInt(ul.children(0).css("margin-top"), 10),
                                        e = $(self).height();
                                    return this.top = c - d - e;
                                },
                                bottom: function() {
                                    var a = parseInt(ul.children(0).css("margin-bottom"), 10);
                                    return this.bottom = a;
                                    return this.bottom = $(self).outerHeight(!0);
                                }
                            }
                        });
                    }
                }

                // Find the last active item set by the scrollspy and set our tag on it
                self.on('activate.bs.scrollspy', function(){
                    var a = $('.nav li.dynamic-scrollspy-title.active');
                    var tag = 'scrollspy-active';
                    $('.' + tag).removeClass(tag);
                    if (a) {
                      a.last().addClass(tag);
                    }
                })

                $('h3.nav.dynamic-scrollspy-heading').on('click', function (e) {
                    $(window).scrollTop(0);
                    e.stopImmediatePropagation();
                    var tag = 'scrollspy-active';
                    $('.' + tag).removeClass(tag);
                })

                $('body').attr('data-spy', 'true').scrollspy({
                    target: '#' + self.prop('id'),
                    offset: self.options.offset
                });

                self.isinit = true;
            } else {
                makeList();
                $('[data-spy="scroll"]').each(function() {
                    $(this).scrollspy('refresh');
                });
            }
            return self;
        }
        return init();
    };
}(window.$jqTheme || window.jQuery));
