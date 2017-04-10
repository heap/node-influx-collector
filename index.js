var EventEmitter = require('events').EventEmitter;
var influx = require('influx');
var url = require('url');

// create a collector for all series
function Collector(uri) {
    if (!(this instanceof Collector)) {
        return new Collector(uri);
    }

    var self = this;

    if (!uri) {
        return;
    }

    var parsed = url.parse(uri, true /* parse query args */);

    var username = undefined;
    var password = undefined;

    if (parsed.auth) {
        var parts = parsed.auth.split(':');
        username = parts.shift();
        password = parts.shift();
    }


    self._client = new influx.InfluxDB({
        host : parsed.hostname,
        port : parsed.port,
        protocol : parsed.protocol.split(':')[0], // remove trailing ':',
        username : username,
        password : password,
        database : parsed.pathname.slice(1), // remove leading '/'
        reportTime : false
    });

    self._series = {};
    self._points = [];
    self._flushesInFlight = 0;

    var opt = parsed.query || {};

    self._instant_flush = opt.instantFlush == 'yes';
    self._time_precision = opt.time_precision;

    // no automatic flush
    if (opt.autoFlush == 'no' || self._instant_flush) {
        return;
    }

    var flush_interval = opt.flushInterval || 5000;

    // flush on an interval
    // or option to auto_flush=false
    self._interval = setInterval(function() {
        self.flush();
    }, flush_interval).unref();
};

Collector.prototype.__proto__ = EventEmitter.prototype;

MTU_SIZE = 1400; // Conservative estimate for the Maximum transmission unit.

Collector.prototype.computePointCountToSend = function(pointSizes, upperBound) {
  var index = 0;
  var sum = 0;
  while (index < pointSizes.length && sum <= upperBound) {
    sum += pointSizes[index];
    ++index;
  }
  if (sum > upperBound) {
    --index;
  }
  index = Math.max(index, 1); // Always send at least one point.
  return Math.min(index, pointSizes.length); // But not if there were no points at all.
};

Collector.prototype._notifyIfFlushed = function(callback, err) {
    if (!callback || this._flushesInFlight > 0) {
        return;
    }
    setImmediate(callback, err);
};

Collector.prototype._flushPoints = function(points, callback) {
    if (!points || points.length === 0) {
        return;
    }
    var self = this;

    // only send N points at a time to avoid making requests too large
    var spliceIndex = 50
    var batch = points.slice(0, spliceIndex);
    points = points.slice(spliceIndex);
    var opt = { precision: self._time_precision };

    self._flushesInFlight++;
    self._client.writePoints(batch, opt)
      .catch(function(err) {
        self._flushesInFlight--;
        // TODO if error put points back to send again?
        self.emit('error', err);
        self._notifyIfFlushed(callback, err);
        return;
      })
      .then(function() {
        self._flushesInFlight--;
        // there are more points to flush out
        if (points.length > 0) {
            self._flushPoints(points);
        }
        self._notifyIfFlushed(callback);
      });
};

Collector.prototype.flush = function(callback) {
    var points = this._points;
    this._points = [];
    this._flushPoints(points, callback);
    this._notifyIfFlushed(callback);
};

// collect a data point (or object)
// @param [Object] value the data
// @param [Object] tags the tags (optional)
Collector.prototype.collect = function(seriesName, value, tags) {
    var point = {
      measurement: seriesName,
      tags: tags,
      fields: { value }
    }
    if (this._instant_flush) {
        this._flushSeries([point]);
    } else {
        this._points.push(point)
    }
};

Collector.prototype.stop = function() {
    clearInterval(this._interval);
    this.flush();
};

module.exports = Collector;
