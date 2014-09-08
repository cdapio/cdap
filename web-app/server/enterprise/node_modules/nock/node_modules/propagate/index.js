function propagate(events, source, dest) {
  if (arguments.length < 3) {
    dest = source;
    source = events;
    events = undefined;
  }

  // events should be an array
  if (events && ! Array.isArray(events)) events = [events];

  var oldEmit =  source.emit;

  source.emit = function(eventType) {
    if (! events || ~events.indexOf(eventType)) {
      dest.emit.apply(dest, arguments);
    }
    oldEmit.apply(source, arguments);
  }

  function end() {
    source.emit = oldEmit;
  }

  return {
    end: end
  };
};

module.exports = propagate;