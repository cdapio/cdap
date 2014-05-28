# -*- coding: utf-8 -*-
# See LICENSE.txt for licensing terms
#$URL: https://rst2pdf.googlecode.com/svn/tags/0.93/rst2pdf/basenodehandler.py $
#$Date: 2012-02-28 21:07:21 -0300 (Tue, 28 Feb 2012) $
#$Revision: 2443 $

'''
This module provides one useful class:  NodeHandler

The NodeHandler class is designed to be subclassed.  Each subclass
should support the processing that createpdf.RstToPdf needs to do
on a particular type of node that could appear in a document tree.

When the subclass is defined, it should reference NodeHandler as
the first base class, and one or more docutils node classes as
subsequent base classes.

These docutils node classes will not actually wind up in the
base classes of the subclass.  Instead, they will be used as
keys in a dispatch dictionary which is used to find the correct
NodeHandler subclass to use to process an instance of a given
docutils node class.

When an instance of createpdf.RstToPdf is created, a NodeHandler
instance will be called to return dispatchers for gather_elements
and gather_pdftext, wrapped up as methods of the createpdf.RstToPdf
class.

When a dispatcher is called, it will dispatch to the correct subclass
to handle the given docutils node instance.

If no NodeHandler subclass has been created to handle that particular
type of docutils node, then default processing will occur and a warning
will be logged.
'''

import types
import inspect
from log import log, nodeid
from smartypants import smartyPants
import docutils.nodes
from flowables import BoundByWidth, TocEntry


class MetaHelper(type):
    ''' MetaHelper is designed to generically enable a few of the benefits of
        using metaclasses by encapsulating some of the complexity of setting
        them up.

        If a base class uses MetaHelper (by assigning __metaclass__ = MetaHelper),
        then that class (and its metaclass inheriting subclasses) can control
        class creation behavior by defining a couple of helper functions.

        1) A base class can define a _classpreinit function.  This function
           is called during __new__ processing of the class object itself,
           but only during subclass creation (not when the class defining
           the _classpreinit is itself created).

           The subclass object does not yet exist at the time _classpreinit
           is called.  _classpreinit accepts all the parameters of the
           __new__ function for the class itself (not the same as the __new__
           function for the instantiation of class objects!) and must return
           a tuple of the same objects.  A typical use of this would be to
           modify the class bases before class creation.

        2) Either a base class or a subclass can define a _classinit() function.
           This function will be called immediately after the actual class has
           been created, and can do whatever setup is required for the class.
           Note that every base class (but not every subclass) which uses
           MetaHelper MUST define _classinit, even if that definition is None.

         MetaHelper also places an attribute into each class created with it.
         _baseclass is set to None if this class has no superclasses which
         also use MetaHelper, or to the first such MetaHelper-using baseclass.
         _baseclass can be explicitly set inside the class definition, in
         which case MetaHelper will not override it.
    '''
    def __new__(clstype, name, bases, clsdict):
        # Our base class is the first base in the class definition which
        # uses MetaHelper, or None if no such base exists.
        base = ([x for x in bases if type(x) is MetaHelper] + [None])[0]

        # Only set our base into the class if it has not been explicitly
        # set
        clsdict.setdefault('_baseclass', base)

        # See if the base class definied a preinit function, and call it
        # if so.
        preinit = getattr(base, '_classpreinit', None)
        if preinit is not None:
            clstype, name, bases, clsdict = preinit(clstype, name, bases, clsdict)

        # Delegate the real work to type
        return type.__new__(clstype, name, bases, clsdict)

    def __init__(cls, name, bases, clsdict):
        # Let type build the class for us
        type.__init__(cls, name, bases, clsdict)
        # Call the class's initialization function if defined
        if cls._classinit is not None:
            cls._classinit()


class NodeHandler(object):
    ''' NodeHandler classes are used to dispatch
       to the correct class to handle some node class
       type, via a dispatchdict in the main class.
    '''
    __metaclass__ = MetaHelper

    @classmethod
    def _classpreinit(baseclass, clstype, name, bases, clsdict):
        # _classpreinit is called before the actual class is built
        # Perform triage on the class bases to separate actual
        # inheritable bases from the target docutils node classes
        # which we want to dispatch for.

        new_bases = []
        targets = []
        for target in bases:
            if target is not object:
                (targets, new_bases)[issubclass(target, NodeHandler)].append(target)
        clsdict['_targets'] = targets
        return clstype, name, tuple(new_bases), clsdict

    @classmethod
    def _classinit(cls):
        # _classinit() is called once the subclass has actually
        # been created.

        # For the base class, just add a dispatch dictionary
        if cls._baseclass is None:
            cls.dispatchdict = {}
            return

        # for subclasses, instantiate them, and then add
        # the class to the dispatch dictionary for each of its targets.
        self = cls()
        for target in cls._targets:
            if cls.dispatchdict.setdefault(target, self) is not self:
                t = repr(target)
                old = repr(cls.dispatchdict[target])
                new = repr(self)
                log.debug('Dispatch handler %s for node type %s overridden by %s' %
                    (old, t, new))
                cls.dispatchdict[target] = self

    @staticmethod
    def getclassname(obj):
        cln = repr(obj.__class__)
        info = cln.split("'")
        if len(info) == 3:
            return info[1]
        return cln

    def log_unknown(self, node, during):
        if not hasattr(self, 'unkn_node'):
            self.unkn_node = set()
        cln=self.getclassname(node)
        if not cln in self.unkn_node:
            self.unkn_node.add(cln)
            log.warning("Unkn. node (self.%s): %s [%s]",
                during, cln, nodeid(node))
            try:
                log.debug(node)
            except (UnicodeDecodeError, UnicodeEncodeError):
                log.debug(repr(node))

    def findsubclass(self, node, during):
        handlerinfo = '%s.%s' % (self.getclassname(self), during)
        log.debug("%s: %s", handlerinfo, self.getclassname(node))
        log.debug("%s: [%s]", handlerinfo, nodeid(node))
        try:
            log.debug("%s: %s", handlerinfo, node)
        except (UnicodeDecodeError, UnicodeEncodeError):
            log.debug("%s: %r", handlerninfo, node)
        log.debug("")

        # Dispatch to the first matching class in the MRO

        dispatchdict = self.dispatchdict
        for baseclass in inspect.getmro(node.__class__):
            result = dispatchdict.get(baseclass)
            if result is not None:
                break
        else:
            self.log_unknown(node, during)
            result = self
        return result

    def __call__(self, client):
        ''' Get the dispatchers, wrapped up as methods for the client'''
        textdispatch = types.MethodType(self.textdispatch, client)
        elemdispatch = types.MethodType(self.elemdispatch, client)
        return textdispatch, elemdispatch

    # This overridable attribute will be set true in the instance
    # if handling a sphinx document

    sphinxmode = False

    # Begin overridable attributes and methods for elemdispatch

    def gather_elements(self, client, node, style):
        return client.gather_elements(node, style=style)

    def getstyle(self, client, node, style):
        try:
            if node['classes'] and node['classes'][0]:
                # FIXME: Supports only one class, sorry ;-)
                if client.styles.StyleSheet.has_key(node['classes'][0]):
                    style = client.styles[node['classes'][0]]
                else:
                    log.info("Unknown class %s, ignoring. [%s]",
                        node['classes'][0], nodeid(node))
        except TypeError: # Happens when a docutils.node.Text reaches here
            pass

        if style is None or style == client.styles['bodytext']:
            style = client.styles.styleForNode(node)
        return style

    def getelements(self, client, node, style):
        style = self.getstyle(client, node, style)
        elements = self.gather_elements(client, node, style)

        # Make all the sidebar cruft unreachable
        #if style.__dict__.get('float','None').lower() !='none':
            #node.elements=[Sidebar(node.elements,style)]
        #elif 'width' in style.__dict__:

        if 'width' in style.__dict__:
            elements = [BoundByWidth(style.width,
                elements, style, mode="shrink")]

        return elements

    # End overridable attributes and methods for elemdispatch

    def elemdispatch(self, client, node, style=None):
        self = self.findsubclass(node, 'elemdispatch')

        # set anchors for internal references
        try:
            for i in node['ids']:
                client.pending_targets.append(i)
        except TypeError: #Happens with docutils.node.Text
            pass

        elements = self.getelements(client, node, style)

        if node.line and client.debugLinesPdf:
            elements.insert(0,TocEntry(client.depth-1,'LINE-%s'%node.line))
        node.elements = elements
        return elements

    # Begin overridable attributes and methods for textdispatch

    pre = ''
    post = ''

    def get_pre_post(self, client, node, replaceEnt):
        return self.pre, self.post

    def get_text(self, client, node, replaceEnt):
        return client.gather_pdftext(node)

    def apply_smartypants(self, text, smarty, node):
        # Try to be clever about when to use smartypants
        if node.__class__ in (docutils.nodes.paragraph,
                docutils.nodes.block_quote, docutils.nodes.title):
            return smartyPants(text, smarty)
        return text

    # End overridable attributes and methods for textdispatch

    def textdispatch(self, client, node, replaceEnt=True):
        self = self.findsubclass(node, 'textdispatch')
        pre, post = self.get_pre_post(client, node, replaceEnt)
        text = self.get_text(client, node, replaceEnt)
        text = pre + text + post

        try:
            log.debug("%s.textdispatch: %s" % (self.getclassname(self), text))
        except UnicodeDecodeError:
            pass

        text = self.apply_smartypants(text, client.smarty, node)
        node.pdftext = text
        return text
