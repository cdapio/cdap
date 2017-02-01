# -*- coding: utf-8 -*-
"""
    javadoc.py
    ----------

    Extension to save typing and prevent hard-coding of base URLs in the reST
    files.
    
    Now you can use :javadoc:`foo` in your documents. 
    
    This will create a link to ``[../]reference-manual/javadocs/foo.html``.
    
    If a link does not end with ``.html``, it will be added.

    If the link starts with ``http`` and ``javadocs`` is found in the link, it will be stripped out:
    
        http://docs.cask.co/cdap/develop/en/reference-manual/javadocs/co/cask/cdap/api/ClientLocalizationContext.html

    becomes
    
        [../]reference-manual/javadocs/co/cask/cdap/api/ClientLocalizationContext.html
    
    allowing for easy copy and paste from existing Javadocs.
    
    You can also give an explicit caption, e.g. :javadoc:`Foo <foo>`.
    
    :copyright: Copyright 2017 Cask Data, Inc.
    :license: Apache License 2.0
"""

from docutils import nodes, utils

from sphinx.util.nodes import split_explicit_title


def make_link_role(base_url, prefix):
    def role(typ, rawtext, text, lineno, inliner, options={}, content=[]):
        text = utils.unescape(text)
        has_explicit_title, title, part = split_explicit_title(text)
        try:
            full_url = base_url % part
        except (TypeError, ValueError):
            inliner.reporter.warning(
                'unable to expand %s extlink with base URL %r, please make '
                'sure the base contains \'%%s\' exactly once'
                % (typ, base_url), line=lineno)
            full_url = base_url + part
        if not has_explicit_title:
            if prefix is None:
                title = full_url
            else:
                title = prefix + part
        pnode = nodes.reference(title, title, internal=False, refuri=full_url)
        return [pnode], []
    return role

def setup_link_roles(app):
    for name, (base_url, prefix) in app.config.extlinks.iteritems():
        app.add_role(name, make_link_role(base_url, prefix))

def setup(app):
    app.add_config_value('javadoc', {}, 'env')
    app.connect('builder-inited', setup_link_roles)
