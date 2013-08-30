* Use DOM.HTMLDocument rather than DOM.Document

* move to a more functional style -- having to call new inBody(blah).method
  rather than inBody.blah stinks

* Work on encoding detection and support. I'm sure this will involve a lot
  of moaning about Javascript's and V8's encoding support in the string
  objects.

* Improve speed. v8's handling of foo[bar]() is slow compared to more static 
  alternatives, so there's a possible place to start.

* Set up an evented serializer, so that as soon as the head is complete and body underway, possibly, start streaming serialized data
