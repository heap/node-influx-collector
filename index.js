var EventEmitter = require('events').EventEmitter;
var influx = require('influx');
var influx_udp = require('influx-udp');
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

    var client = parsed.protocol == 'udp:' ? influx_udp : influx;
    var protocol = parsed.protocol == 'udp:' ? 'line' : parsed.protocol;

    self._client = new client({
        host : parsed.hostname,
        port : parsed.port,
        protocol : protocol,
        username : username,
        password : password,
        database : parsed.pathname.slice(1), // remove leading '/'
        reportTime : false
    });

    self._series = {};
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
}

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
    self = this;
    if (!callback || self._flushesInFlight > 0) {
        return;
    }
    setImmediate(callback, err);
}

Collector.prototype._flushSeries = function(seriesName, points, callback) {
    if (!points || points.length === 0) {
        return;
    }
    self = this;

    // only send N points at a time to avoid making requests too large
    var spliceIndex;
    if (self._client.protocol == 'udp:') {
      spliceIndex = self.computePointCountToSend(self._points.map(function (point) {
        return JSON.stringify(point).length;
      }), MTU_SIZE);
    } else {
      spliceIndex = 50;
    }
    var batch = points.splice(0, spliceIndex);
    var opt = { precision: self._time_precision };

    self._flushesInFlight++;
    self._client.writePoints(seriesName, batch, opt, function(err) {
        self._flushesInFlight--;
        if (err) {
            // TODO if error put points back to send again?
            self.emit('error', err);
            self._notifyIfFlushed(callback, err);
            return;
        }

        // there are more points to flush out
        if (points.length > 0) {
            self._flushSeries(seriesName, points);
        }
        self._notifyIfFlushed(callback);
    });
}

Collector.prototype.flush = function(callback) {
    var self = this;

    Object.keys(self._series).forEach(function(key) {
        var series = self._series[key];
        delete self._series[key];

        self._flushSeries(key, series, callback);
    });
    self._notifyIfFlushed(callback);
};

function getSeries(collector, name, reset) {
    var series = collector._series[name];

    if(!series) {
        series = [];
        collector._series[name] = series;
    }

    return series;
}

// collect a data point (or object)
// @param [Object] value the data
// @param [Object] tags the tags (optional)
Collector.prototype.collect = function(seriesName, value, tags) {
    var self = this;

    if (self._instant_flush) {
        self._flushSeries(seriesName, [[value, tags]]);
    } else {
        var series = getSeries(self, seriesName);

        series.push([value, tags]);
    }
};

Collector.prototype.stop = function() {
    var self = this;

    clearInterval(self._interval);
    self.flush();
}

module.exports = Collector;
