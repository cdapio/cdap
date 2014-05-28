# -*- coding: utf-8 -*-

# Copyright (C) 2007-2008 by Igor Novikov
# Copyright (C) 2000, 2001, 2002 by Bernhard Herzog
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Library General Public
# License as published by the Free Software Foundation; either
# version 2 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

###Sketch Config
#type = Export
#tk_file_type = ("Portable Document Format (PDF)", '.pdf')
#extensions = '.pdf'
#format_name = 'PDF-Reportlab'
#unload = 1
###End

from math import atan2, pi
import PIL
from app import _,Bezier, EmptyPattern, Rotation, Translation, _sketch
from app.Graphics.curveop import arrow_trafos
import reportlab.pdfgen.canvas
import app


def make_pdf_path(pdfpath, paths):
	for path in paths:
		for i in range(path.len):
			type, control, p, cont = path.Segment(i)
			if type == Bezier:
				p1, p2 = control
				pdfpath.curveTo(p1.x, p1.y, p2.x, p2.y, p.x, p.y)
			else:
				if i > 0:
					pdfpath.lineTo(p.x, p.y)
				else:
					pdfpath.moveTo(p.x, p.y)
		if path.closed:
			pdfpath.close()
	return pdfpath

class PDFDevice:

	has_axial_gradient = 0
	has_radial_gradient = 0
	has_conical_gradient = 0
	gradient_steps = 100

	def __init__(self, pdf):
		self.pdf = pdf

	def PushTrafo(self):
		self.pdf.saveState()

	def Concat(self, trafo):
		apply(self.pdf.transform, trafo.coeff())

	def Translate(self, x, y = None):
		if y is None:
			x, y = x
		self.pdf.translate(x, y)

	def Rotate(self, angle):
		self.pdf.rotate(angle)

	def Scale(self, scale):
		self.pdf.scale(scale, scale)
	
	def PopTrafo(self):
		self.pdf.restoreState()

	PushClip = PushTrafo
	PopClip = PopTrafo

	def SetFillColor(self, color):
		self.pdf.setFillColor(tuple(color))

	def SetLineColor(self, color):
		self.pdf.setStrokeColor(tuple(color))

	def SetLineAttributes(self, width, cap = 1, join = 0, dashes = ()):
		self.pdf.setLineWidth(width)
		self.pdf.setLineCap(cap - 1)
		self.pdf.setLineJoin(join)
		if dashes:
			dashes = list(dashes)
			w = width
			if w < 1.0:
				w = 1.0
			for i in range(len(dashes)):
				dashes[i] = w * dashes[i]
		self.pdf.setDash(dashes)

	def DrawLine(self, start, end):
		self.pdf.line(start.x, start.y, end.x, end.y)

	def DrawLineXY(self, x1, y1, x2, y2):
		self.pdf.line(x1, y1, x2, y2)

	def DrawRectangle(self, start, end):
		self.pdf.rectangle(start.x, start.y, end.x - start.x, end.y - start.y, 
						   1, 0)

	def FillRectangle(self, left, bottom, right, top):
		self.pdf.rect(left, bottom, right - left, top - bottom, 0, 1)

	def DrawCircle(self, center, radius):
		self.pdf.circle(center.x, center.y, radius, 1, 0)

	def FillCircle(self, center, radius):
		self.pdf.circle(center.x, center.y, radius, 0, 1)

	def FillPolygon(self, pts):
		path = self.pdf.beginPath()
		apply(path.moveTo, pts[0])
		for x, y in pts:
			path.lineTo(x, y)
		path.close()
		self.pdf.drawPath(path, 0, 1)

	def DrawBezierPath(self, path, rect = None):
		self.pdf.drawPath(make_pdf_path(self.pdf.beginPath(), (path,)), 1, 0)

	def FillBezierPath(self, path, rect = None):
		self.pdf.drawPath(make_pdf_path(self.pdf.beginPath(), (path,)), 0, 1)



class PDFGenSaver:

	def __init__(self, file, filename, document, options):
		self.file = file
		self.filename = filename
		self.document = document
		self.options = options

		# if there's a pdfgen_canvas option assume it's an instance of
		# reportlab.pdfgen.canvas.Canvas that we should render on. This
		# allows multiple documents to be rendered into the same PDF
		# file or to have other python code outside of Sketch such as
		# reportlab itself (more precisely one of its other components
		# besides pdfgen) render into to too.
		#
		# The code here assumes that the canvas is already setup
		# properly.
		if options.has_key("pdfgen_canvas"):
			self.pdf = options["pdfgen_canvas"]
		else:
			self.pdf = reportlab.pdfgen.canvas.Canvas(file)
			self.pdf.setPageSize(document.PageSize())

	def close(self):
		if not self.options.has_key("pdfgen_canvas"):
			self.pdf.save()

	def set_properties(self, properties, bounding_rect = None):
		pattern = properties.line_pattern
		if not pattern.is_Empty:
			if pattern.is_Solid:
				c, m, y, k =pattern.Color().getCMYK()
				self.pdf.setStrokeColorCMYK(c, m, y, k)
			self.pdf.setLineWidth(properties.line_width)
			self.pdf.setLineJoin(properties.line_join)
			self.pdf.setLineCap(properties.line_cap - 1)
			dashes = properties.line_dashes
			if dashes:
				dashes = list(dashes)
				w = properties.line_width
				if w < 1.0:
					w = 1.0
				for i in range(len(dashes)):
					dashes[i] = w * dashes[i]
			self.pdf.setDash(dashes)
		active_fill = None
		pattern = properties.fill_pattern
		if not pattern.is_Empty:
			if pattern.is_Solid:
				c, m, y, k =pattern.Color().getCMYK()
				self.pdf.setFillColorCMYK(c, m, y, k)
			elif pattern.is_Tiled:
				pass
			elif pattern.is_AxialGradient:
				active_fill = self.axial_gradient
			else:
				active_fill = self.execute_pattern
		return active_fill

	def axial_gradient(self, properties, rect):
		pattern = properties.fill_pattern
		vx, vy = pattern.Direction()
		angle = atan2(vy, vx) - pi / 2
		center = rect.center()
		rot = Rotation(angle, center)
		left, bottom, right, top = rot(rect)
		trafo = rot(Translation(center))
		image = PIL.Image.new('RGB', (1, 200))
		border = int(round(100 * pattern.Border()))
		_sketch.fill_axial_gradient(image.im, pattern.Gradient().Colors(), 
									0, border, 0, 200 - border)
		self.pdf.saveState()
		apply(self.pdf.transform, trafo.coeff())
		self.pdf.drawInlineImage(image, (left - right) / 2, (bottom - top) / 2, 
								 right - left, top - bottom)
		self.pdf.restoreState()

	def execute_pattern(self, properties, rect):
		device = PDFDevice(self.pdf)
		properties.fill_pattern.Execute(device, rect)

	def make_pdf_path(self, paths):
		return make_pdf_path(self.pdf.beginPath(), paths)

	def polybezier(self, paths, properties, bounding_rect, clip = 0):
		pdfpath = self.make_pdf_path(paths)
		active_fill = self.set_properties(properties, bounding_rect)
		if active_fill:
			if not clip:
				self.pdf.saveState()
			self.pdf.clipPath(pdfpath, 0, 0)
			active_fill(properties, bounding_rect)
			if not clip:
				self.pdf.restoreState()
			if properties.HasLine():
				self.pdf.drawPath(pdfpath, 1, 0)
		else:
			if clip:
				method = self.pdf.clipPath
			else:
				method = self.pdf.drawPath
			method(self.make_pdf_path(paths), properties.HasLine(), 
				   properties.HasFill())
		# draw the arrows
		if properties.HasLine():
			# Set the pdf fill color to the line color to make sure that
			# arrows that are filled are filled with the line color of
			# the object. Since lines are always drawn last, this
			# shouldn't interfere with the object's fill.
			c, m, y, k = properties.line_pattern.Color().getCMYK()
			self.pdf.setFillColorCMYK(c, m, y, k)
			arrow1 = properties.line_arrow1
			arrow2 = properties.line_arrow2
			if arrow1 or arrow2:
				for path in paths:
					t1, t2 = arrow_trafos(path, properties)
					if arrow1 and t1 is not None:
						self.draw_arrow(arrow1, t1)
					if arrow2 and t2 is not None:
						self.draw_arrow(arrow2, t2)
					
	def draw_arrow(self, arrow, trafo):
		path = arrow.Paths()[0].Duplicate()
		path.Transform(trafo)
		pdfpath = self.make_pdf_path((path,))
		if arrow.IsFilled():
			self.pdf.drawPath(pdfpath, 0, 1)
		else:
			self.pdf.drawPath(pdfpath, 1, 0)

	def mask_group(self, object):
		mask = object.Mask()
		if not mask.has_properties:
			# XXX implement this case (raster images)
			return
		if mask.is_curve:
			self.pdf.saveState()
			prop = mask.Properties().Duplicate()
			prop.SetProperty(line_pattern = EmptyPattern)
			self.polybezier(mask.Paths(), prop, mask.bounding_rect, clip = 1)
			self.save_objects(object.MaskedObjects())
			if mask.has_line and mask.Properties().HasLine():
				prop = mask.Properties().Duplicate()
				prop.SetProperty(fill_pattern = EmptyPattern)
				self.polybezier(mask.Paths(), prop, mask.bounding_rect, 
								clip = 1)
			self.pdf.restoreState()

	def raster_image(self, object):
		self.pdf.saveState()
		apply(self.pdf.transform, object.Trafo().coeff())
		self.pdf.drawInlineImage(object.Data().Image(), 0, 0)
		self.pdf.restoreState()

	def simple_text(self, object, clip = 0):
		properties = object.Properties()
		active_fill = self.set_properties(properties, object.bounding_rect)
		fontname = properties.font.PostScriptName()
		if fontname not in self.pdf.getAvailableFonts():
			fontname = 'Times-Roman'

		if active_fill and not clip:
			self.pdf.saveState()
		pdftext = self.pdf.beginText()
		if active_fill:
			pdftext.setTextRenderMode(7)
		elif clip:
			pdftext.setTextRenderMode(4)
		pdftext.setFont(fontname, properties.font_size)
		apply(pdftext.setTextTransform, object.FullTrafo().coeff())
		pdftext.textOut(object.Text())
		self.pdf.drawText(pdftext)
		if active_fill:
			active_fill(properties, object.bounding_rect)
			if not clip:
				self.pdf.restoreState()

	def path_text(self, object, clip = 0):
		properties = object.Properties()
		active_fill = self.set_properties(properties, object.bounding_rect)
		fontname = properties.font.PostScriptName()
		if fontname not in self.pdf.getAvailableFonts():
			fontname = 'Times-Roman'

		if active_fill and not clip:
			self.pdf.saveState()
		pdftext = self.pdf.beginText()
		if active_fill:
			pdftext.setTextRenderMode(7)
		elif clip:
			pdftext.setTextRenderMode(4)
		pdftext.setFont(fontname, properties.font_size)
		trafos = object.CharacterTransformations()
		text = object.Text()
		for i in range(len(trafos)):
			apply(pdftext.setTextTransform, trafos[i].coeff())
			pdftext.textOut(text[i])
		self.pdf.drawText(pdftext)
		if active_fill:
			active_fill(properties, object.bounding_rect)
			if not clip:
				self.pdf.restoreState()

	def Save(self):
		self.document.updateActivePage()
		masters=self.document.getMasterLayers()
		count=0
		pagenum=len(self.document.pages)
		interval=int(97/pagenum)
		for page in self.document.pages:
			count+=1
			app.updateInfo(inf2=_('Composing page %u of %u')%(count,pagenum),inf3=count*interval)
			layers=page+masters
			for layer in layers:
				if not layer.is_SpecialLayer and layer.Printable():
					self.save_objects(layer.GetObjects())
			#self.pdf.showPage()

	def save_objects(self, objects):
		for object in objects:
			if object.is_Compound:
				if object.is_MaskGroup:
					self.mask_group(object)
				else:
					self.save_objects(object.GetObjects())
			elif object.is_SimpleText:
#				self.simple_text(object)
				obj=object.AsBezier()
				self.polybezier(obj.Paths(), obj.Properties(), obj.bounding_rect)
			elif object.is_PathTextText:
				self.path_text(object)				
			elif object.is_Image:
				self.raster_image(object)
			elif object.is_Bezier or object.is_Rectangle or object.is_Ellipse:
				self.polybezier(object.Paths(), object.Properties(), object.bounding_rect)



def save(document, file, filename, options = {}):
	app.updateInfo(inf1=_('PDF generation.'),inf2=_('Start document composing'),inf3=3)
	saver = PDFGenSaver(file, filename, document, options)
	saver.Save()
	saver.close()
	app.updateInfo(inf2=_('Document generation is finished'),inf3=100)
