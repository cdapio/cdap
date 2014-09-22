#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sphinx.application import ExtensionError

def add_ga_javascript(app, pagename, templatename, context, doctree):
    if not app.config.googleanalytics_enabled:
        return

    metatags = context.get('metatags', '')
    metatags += """<script type="text/javascript">

      var _gaq = _gaq || [];
      _gaq.push(['_setAccount', '%s']);
      _gaq.push(['_trackPageview']);

      (function() {
        var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
        ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
        var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
      })();
    </script>""" % app.config.googleanalytics_id
    context['metatags'] = metatags

def check_config(app):
    if not app.config.googleanalytics_id:
        raise ExtensionError("'googleanalytics_id' config value must be set for ga statistics to function properly.")

def setup(app):
    app.add_config_value('googleanalytics_id', '', 'html')
    app.add_config_value('googleanalytics_enabled', True, 'html')
    app.connect('html-page-context', add_ga_javascript)
    app.connect('builder-inited', check_config)
    return app
