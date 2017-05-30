(function($) {
    // Determines if an element has scrolled into view


    function elementScrolled(elem) {
        var docViewTop = $(window).scrollTop();
        var docViewBottom = docViewTop + $(window).height();
        var elemTop = $(elem).offset().top;
        return ((elemTop <= docViewBottom) && (elemTop >= docViewTop));
    }

    // Disables the sidebar scrollbar on small screens


    function sidebar_scrollbar_adjust() {
        // Bootstrap break points
        if (window.matchMedia('(min-width: 992px)').matches) {
            $('#sidebar').mCustomScrollbar('update');
        } else {
            $('#sidebar').mCustomScrollbar('disable', true);
        }
    }

    // Adjustments for the left and right sidebars


    function elem_adjust() {
        var s_margin_left = 'inherit';
        var s_position = 'relative';
        var s_width = '100%';
        // Bootstrap break point
        if (window.matchMedia('(min-width: 992px)').matches) {
            s_margin_left = '-16px';
            s_position = 'fixed';
            s_width = parseInt($('div.main-container.container > .row').width() * 0.1666666667) + 'px';

            var maxRightSidebarHeight;
            // Calculate maximum sidebar height based on page size
            var winH = $(window).height();
            var sidebarTop = 140;
            var spaceToFoot = 60;
            var maxPageSidebarHeight = winH - sidebarTop - spaceToFoot;
            maxRightSidebarHeight = maxPageSidebarHeight;
            // Calculate maximum sidebar height based on content size
            var rightSidebarContentOuterHeight = $('#localtoc-scrollspy').outerHeight();
            if (rightSidebarContentOuterHeight < maxPageSidebarHeight) {
              maxRightSidebarHeight = rightSidebarContentOuterHeight
            }
        }
        if ($('#localtoc-scrollspy').children().length > 0) {
          $('#right-sidebar').css('border-left-color', '#ff6600');
        }
        $('#sidebar').css('margin-left', s_margin_left).css('position', s_position).css('width', s_width);
        $('#right-sidebar').css('position', s_position).css('width', s_width).height(maxRightSidebarHeight);
    }

    $(window).on('load scroll resize', function() {
        elem_adjust();
        sidebar_scrollbar_adjust();
    });

    $('#sidebar').affix({
        offset: {
            top: 0
        }
    });

    // Add collapsible menus to left  sidebar

    $(document).ready(function() {
            $('#sidebar nav.pagenav ul.current').abixTreeList();
    });

    $('#localtoc-scrollspy').DynamicScrollSpy({
        affix: false,
        genIDs: true,
        offset: 140,
        startingID: '#main-content',
        testing: false,
        ulClassNames: 'hidden-print page-nav'
    });

    $('.scrollable-x').mCustomScrollbar({
        axis: 'x',
        callbacks: {
            onUpdate: function() {
                $(this).mCustomScrollbar('scrollTo', 'left');
            }
        },
        mouseWheel: false,
        scrollInertia: 0,
        theme: 'minimal-dark'
    });

    $('.scrollable-y').mCustomScrollbar({
        axis: 'y',
        callbacks: {
            onUpdate: function() {
                $(this).mCustomScrollbar('scrollTo', 'top');
            }
        },
        scrollInertia: 0,
        theme: 'minimal-dark'
    });

    $('.scrollable-y-outside').mCustomScrollbar({
        axis: 'y',
        callbacks: {
            onUpdate: function() {
                $(this).mCustomScrollbar('scrollTo', 'top');
            }
        },
        scrollbarPosition: 'outside',
        scrollInertia: 0,
        theme: 'minimal-dark'
    });

    $('pre').mCustomScrollbar({
        axis: 'x',
        callbacks: {
            onUpdate: function() {
                $(this).mCustomScrollbar('scrollTo', 'left');
            }
        },
        mouseWheel: false,
        scrollInertia: 0,
        theme: 'minimal-dark'
    });

    // Load Google Search Data

    function load_google_version() {
        var versionsURL = 'http://docs.cask.co/cdap/';

        window.versionscallback = (function(data) {
            var ess;
            if (data) {
                document.write('<li class="versionsmenu">');
                document.write('<select id="' + versionID + '" onmousedown="window.currentversion=this.value;" onchange="window.gotoVersion(\'' + versionID + '\')">');
            }

            if (Array.isArray(data.development) && data.development.length && data.development[0].length) {
                ess = (data.development.length == 1) ? '' : 's';
                document.write('<optgroup label="Development Release' + ess + '">');
                var i;
                var d;
                for (i in data.development) {
                    d = data.development[i];
                    if (d.length === 2 || (d.length > 2 && parseInt(d[3]) === 1)) {
                        writeVersionLink(d[0], d[1]);
                    }
                }
                document.write('</optgroup>');
            } else {
                writeLink('develop', 'Develop');
            }

            document.write('<optgroup label="Current Release">');
            if (Array.isArray(data.current) && data.current.length && data.current[0].length) {
                writeVersionLink(data.current[0], data.current[1]);
            } else {
                writeLink('current', 'Current');
            }
            document.write('</optgroup>');

            if (Array.isArray(data.older) && data.older.length && data.older[0].length) {
                ess = (data.older.length == 1) ? '' : 's';
                document.write('<optgroup label="Older Release' + ess + '">');
                var j;
                var r;
                for (j in data.older) {
                    r = data.older[j];
                    if (parseInt(r[3]) === 1) {
                        if (r.length === 4 || (r.length > 4 && !parseInt(r[4]) === 0)) {
                            writeVersionLink(r[0], r[1]);
                        }
                    }
                }
                document.write('<option value="' + versionsURL + '">All Releases</option>');
                document.write('</optgroup>');
            }

            if (data) {
                document.write('</select>');
            }

            var gcseID = 'DEFAULT_GCSE_ID';
            if (data && data.gcse) {
                var v = window.DOCUMENTATION_OPTIONS.VERSION;
                if (!(data.gcse[v]) && v.endsWith('-SNAPSHOT')) {
                    v = v.replace('-SNAPSHOT', '');
                }
                if (data.gcse[v]) {
                    gcseID = data.gcse[v];
                }
            }
            localStorage.setItem('gcseID', gcseID);
        });
    }

}(window.$jqTheme || window.jQuery));
