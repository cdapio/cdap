#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This module defines a directive, youtube. It takes a single, required argument, a YouTube video ID:
# 
# ..  youtube:: oHg5SJYRHA0
# 
# The referenced video will be embedded into HTML output. By default, the embedded video
# will be sized for 720p content. To control this, the parameters "aspect", "width", and
# "height" may optionally be provided:
# 
# ..  youtube:: oHg5SJYRHA0
#     :width: 640
#     :height: 480
# 
# ..  youtube:: oHg5SJYRHA0
#     :aspect: 4:3
# 
# ..  youtube:: oHg5SJYRHA0
#     :width: 100%
# 
# ..  youtube:: oHg5SJYRHA0
#     :height: 200px
#
# Based on
# https://bitbucket.org/birkenfeld/sphinx-contrib/src/7f6656c87d3511e6f21670b61c506d0b28598c2a/youtube/?at=default
#
# Added support for an "align" parameter ("left" (default), "right", "center" or "centre")
#

from __future__ import division

import re
from docutils import nodes
from docutils.parsers.rst import directives
from sphinx.util.compat import Directive

CONTROL_HEIGHT = 30

def get_size(d, key):
    if key not in d:
        return None
    m = re.match("(\d+)(|%|px)$", d[key])
    if not m:
        raise ValueError("invalid size %r" % d[key])
    return int(m.group(1)), m.group(2) or "px"

def css(d):
    return "; ".join(sorted("%s: %s" % kv for kv in d.iteritems()))

def combine(dict1, dict2):
    combined_dict = dict1.copy()
    combined_dict.update(dict2)
    return combined_dict


class youtube(nodes.General, nodes.Element): pass


def visit_youtube_node(self, node):
    aspect = node["aspect"]
    width = node["width"]
    height = node["height"]
    align = node["align"]

    style_margins = {
        'margin-left': 'auto',
        'margin-right': 'auto',
        'display': 'block',
    }
    if align == 'right':
        style_margins['margin-right'] = '0'
    elif align == 'left':
        style_margins['margin-left'] = '0'

    if aspect is None:
        aspect = 16, 9

    if (height is None) and (width is not None) and (width[1] == "%"):
        style = {
            "padding-top": "%dpx" % CONTROL_HEIGHT,
            "padding-bottom": "%f%%" % (width[0] * aspect[1] / aspect[0]),
            "width": "%d%s" % width,
            "position": "relative",
        }
        self.body.append(self.starttag(node, "div", style=css(style)))
        style = {
            "position": "absolute",
            "top": "0",
            "left": "0",
            "width": "100%",
            "height": "100%",
            "border": "0",
        }
        style = combine(style, style_margins)
        attrs = {
            "src": "https://www.youtube.com/embed/%s" % node["id"],
            "style": css(style),
        }
        self.body.append(self.starttag(node, "iframe", **attrs))
        self.body.append("</iframe></div>")
    else:
        if width is None:
            if height is None:
                width = 560, "px"
            else:
                width = height[0] * aspect[0] / aspect[1], "px"
        if height is None:
            height = width[0] * aspect[1] / aspect[0], "px"
        style = {
            "width": "%d%s" % width,
            "height": "%d%s" % (height[0] + CONTROL_HEIGHT, height[1]),
            "border": "0",
        }
        style = combine(style, style_margins)
        attrs = {
            "src": "https://www.youtube.com/embed/%s" % node["id"],
            "style": css(style),
        }
        self.body.append(self.starttag(node, "iframe", **attrs))
        self.body.append("</iframe>")

def depart_youtube_node(self, node):
    pass

class YouTube(Directive):
    ALIGN_KEYS = ['left', 'right', 'center', 'centre']
    has_content = True
    required_arguments = 1
    optional_arguments = 0
    final_argument_whitespace = False
    option_spec = {
        "width": directives.unchanged,
        "height": directives.unchanged,
        "aspect": directives.unchanged,
        "align": directives.unchanged,
    }

    def run(self):
        if "aspect" in self.options:
            aspect = self.options.get("aspect")
            m = re.match("(\d+):(\d+)", aspect)
            if m is None:
                raise ValueError("invalid aspect ratio %r" % aspect)
            aspect = tuple(int(x) for x in m.groups())
        else:
            aspect = None
        width = get_size(self.options, "width")
        height = get_size(self.options, "height")
        if "align" in self.options and self.options.get("align") in self.ALIGN_KEYS:
            align = self.options.get("align")
        else:
            align = 'left'
        return [youtube(id=self.arguments[0], aspect=aspect, width=width, height=height, align=align)]

def setup(app):
    app.add_node(youtube, html=(visit_youtube_node, depart_youtube_node))
    app.add_directive("youtube", YouTube)
