var assert = require('assert');
var simple = require('simple-mock')

var Collector = require('../');

var COLLECTOR_URL = 'http://user:password@example.com:8086/test-db';

describe('Influx Collector', function() {
  it('should create a collector', function(done) {
      var statsCollector = Collector(COLLECTOR_URL);
      done();
  });

  it('should collect a single data point with a primative value', function(done) {
      var statsCollector = Collector(COLLECTOR_URL);
      var writePointsSpy = simple.mock(statsCollector._client, 'writePoints');

      statsCollector.collect('series-name', 35);
      statsCollector.flush();

      assert.equal(writePointsSpy.callCount, 1);
      var writeArgs = [
        [
          {
            measurement: 'series-name',
            tags: undefined,
            fields: {value: 35}
          }
        ],
        {precision: undefined}
      ];
      assert.deepEqual(writePointsSpy.calls[0].args, writeArgs);
      done();
  });

  it('should collect a single data point with an object value', function(done) {
      var statsCollector = Collector(COLLECTOR_URL);
      var writePointsSpy = simple.mock(statsCollector._client, 'writePoints');

      statsCollector.collect('series-name', {foo: 34.0});
      statsCollector.flush();

      assert.equal(writePointsSpy.callCount, 1);
      var writeArgs = [
        [
          {
            measurement: 'series-name',
            tags: undefined,
            fields: {foo: 34}
          }
        ],
        {precision: undefined}
      ];
      assert.deepEqual(writePointsSpy.calls[0].args, writeArgs);
      done();
  });

  it('should collect a single data point with tags', function(done) {
      var statsCollector = Collector(COLLECTOR_URL);
      var writePointsSpy = simple.mock(statsCollector._client, 'writePoints');

      statsCollector.collect('series-name', {foo: 34.0}, {host: 'cat'});
      statsCollector.flush();

      assert.equal(writePointsSpy.callCount, 1);
      var writeArgs = [
        [
          {
            measurement: 'series-name',
            tags: {host: 'cat'},
            fields: {foo: 34}
          }
        ],
        {precision: undefined}
      ];
      assert.deepEqual(writePointsSpy.calls[0].args, writeArgs);
      done();
  });

  it('should collect multiple data points', function(done) {
      var statsCollector = Collector(COLLECTOR_URL);
      var writePointsSpy = simple.mock(statsCollector._client, 'writePoints');

      statsCollector.collect('series-name', {bar: 'cat', foo: 34.0});
      statsCollector.collect('series-name2', {foo: 10.2, bar: 'dog'});
      statsCollector.flush();

      assert.equal(writePointsSpy.callCount, 1);
      var writeArgs = [
        [
          {
            measurement: 'series-name',
            tags: undefined,
            fields: {bar: 'cat', foo: 34.0}
          },
          {
            measurement: 'series-name2',
            tags: undefined,
            fields: {bar: 'dog', foo: 10.2}
          }
        ],
        {precision: undefined}
      ];
      assert.deepEqual(writePointsSpy.calls[0].args, writeArgs);
      done();
  });

  it('should support instantFlush', function(done) {
      var statsCollector = Collector(COLLECTOR_URL + '?instantFlush=yes');
      var writePointsSpy = simple.mock(statsCollector._client, 'writePoints');

      statsCollector.collect('series-name', {foo: 34.0});

      assert.equal(writePointsSpy.callCount, 1);
      var writeArgs = [
        [
          {
            measurement: 'series-name',
            tags: undefined,
            fields: {foo: 34.0}
          }
        ],
        {precision: undefined}
      ];
      assert.deepEqual(writePointsSpy.calls[0].args, writeArgs);
      done();
  });

  it('should support a custom flushInterval', function(done) {
      var statsCollector = Collector(COLLECTOR_URL + '?flushInterval=500');
      var writePointsSpy = simple.mock(statsCollector._client, 'writePoints');

      statsCollector.collect('series-name', {foo: 34.0});

      assert.equal(writePointsSpy.callCount, 0);
      setTimeout(function() {
        assert.equal(writePointsSpy.callCount, 1);
        var writeArgs = [
          [
            {
              measurement: 'series-name',
              tags: undefined,
              fields: {foo: 34.0}
            }
          ],
          {precision: undefined}
        ];
        assert.deepEqual(writePointsSpy.calls[0].args, writeArgs);
        done();
      }, 600);
  });

  it('should support a autoFlush=no', function(done) {
      var statsCollector = Collector(COLLECTOR_URL + '?flushInterval=500&autoFlush=no');
      var writePointsSpy = simple.mock(statsCollector._client, 'writePoints');

      statsCollector.collect('series-name', {foo: 34.0});

      assert.equal(writePointsSpy.callCount, 0);
      setTimeout(function() {
        assert.equal(writePointsSpy.callCount, 0);
        statsCollector.flush();
        assert.equal(writePointsSpy.callCount, 1);
        var writeArgs = [
          [
            {
              measurement: 'series-name',
              tags: undefined,
              fields: {foo: 34.0}
            }
          ],
          {precision: undefined}
        ];
        assert.deepEqual(writePointsSpy.calls[0].args, writeArgs);
        done();
      }, 600);
  });

  it('should support time_precision', function(done) {
      var statsCollector = Collector(COLLECTOR_URL + '?time_precision=s');
      var writePointsSpy = simple.mock(statsCollector._client, 'writePoints');

      statsCollector.collect('series-name', {time: 1416512521, foo: 34.0});
      statsCollector.flush();

      assert.equal(writePointsSpy.callCount, 1);
      var writeArgs = [
        [
          {
            measurement: 'series-name',
            tags: undefined,
            fields: {time: 1416512521, foo: 34.0}
          }
        ],
        {precision: 's'}
      ];
      assert.deepEqual(writePointsSpy.calls[0].args, writeArgs);
      done();
  });
});
