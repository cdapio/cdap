# -*- coding: utf-8 -*-
"""
    relative-refs.py
    ----------------

    Extension to save typing and prevent hard-coding of base URLs in the reST files.
    
    Add to the config.py these two definitions:
    
    extensions = [
        'relative-refs',
        ]
    
    relative-refs = {
        'javadoc': ('reference-manual/javadocs', 'http://docs.cask.co/cdap/current/en/reference-manual/javadocs/'),
    }
    
    "relative-refs" is a dictionary naming each "relative ref", and giving the reference location and a prefix.
    
    The reference location is the place to start from in the eventual doc root. It normally would start with a manual.
    The prefix is any text that might appear on the start of references that should be removed.
      
    With the above, you can use in a document:
    
        :javadoc:`foo`
    
    This will create a link to ``[../]reference-manual/javadocs/foo.html``, adding as many "../" as required.
    
    If a link does not end with ``.html``, it will be added.

    If prefix is defined and the link starts with the prefix, it will be stripped off:
    
        http://docs.cask.co/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/ClientLocalizationContext.html

    becomes
    
        [../]reference-manual/javadocs/co/cask/cdap/api/ClientLocalizationContext.html
    
    This allows for easy copy and paste from existing Javadocs.
    
    You can also give an explicit caption, such as:
    
        :javadoc:`Foo <foo>`
      
    Examples: (All will produce the same final references, but with different titles shown)
    
    :javadoc:`http://docs.cask.co/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/ClientLocalizationContext.html`

    :javadoc:`<http://docs.cask.co/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/ClientLocalizationContext.html>`

    :javadoc:`ClientLocalizationContext <http://docs.cask.co/cdap/current/en/reference-manual/javadocs/co/cask/cdap/api/ClientLocalizationContext.html>`

    :javadoc:`ClientLocalizationContext <co/cask/cdap/api/ClientLocalizationContext>`

    :javadoc:`co/cask/cdap/api/ClientLocalizationContext`

    :javadoc:`<co/cask/cdap/api/ClientLocalizationContext>`

    :copyright: Copyright 2017 Cask Data, Inc.
    :license: Apache License 2.0
"""

from os import path

from docutils import nodes, utils
from docutils.utils import relative_path, unescape
from sphinx.util.nodes import split_explicit_title

def make_link_role(base_url, prefix):
    def role(typ, rawtext, text, lineno, inliner, options={}, content=[]):
        text = utils.unescape(text)
        has_explicit_title, title, part = split_explicit_title(text)
        if not has_explicit_title:
            if part.startswith('<') and part.endswith('>'):
                part = part[1:-1]
            title = part
        if prefix and part.startswith(prefix):
            part = part[len(prefix):]
        if part.find('.html') == -1:
            part = part + '.html'
        env = inliner.document.settings.env
        # Start from the source dir, go up two directories to the main level, and then add the base URL
        base_url_path = path.join(env.srcdir, '../..', base_url)
        rel_url = path.join(relative_path(env.docname, base_url_path), part)
#         inliner.reporter.warning("rel_url: %s" % rel_url)
        pnode = nodes.reference(title, title, internal=False, refuri=rel_url)
        return [pnode], []
    return role

def setup_link_roles(app):
    for name, (base_url, prefix) in app.config.relative_refs.iteritems():
        app.add_role(name, make_link_role(base_url, prefix))

def setup(app):
    app.add_config_value('relative_refs', {}, 'env')
    app.connect('builder-inited', setup_link_roles)
