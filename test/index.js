
var nock = require('nock');
var mock_udp = require('mock-udp');
var assert = require('assert');

var Collector = require('../');

suite('influx-collector');

var COLLECTOR_URL = 'http://user:password@example.com:8086/test-db';
var UDP_COLLECTOR_URL = 'udp://user:password@example.com:8086/db-defined-in-server-config';

test('should create a collector', function(done) {
    Collector(COLLECTOR_URL);
    done();
});

test('should collect a single data point', function(done) {
    var stats = Collector(COLLECTOR_URL);

    var req = nock('http://example.com:8086')
    .filteringRequestBody(/.*/, function(body) {
        return '*';
    })
    .post('/write', '*')
    .query({db: 'test-db', u: 'user', p: 'password', precision: 'ms', database: 'test-db' })
    .reply(200, function(uri, request_body) {
        assert.equal(request_body, 'series foo=34');
    })

    stats.collect('series', { foo: 34.0 })

    stats.flush();
    setTimeout(function() {
        req.done();
        done();
    }, 100);
});

test('should collect a single data point via UDP', function(done) {
    var stats = Collector(UDP_COLLECTOR_URL);

    var scope = mock_udp('example.com:8086');

    stats.collect(
        'series-name',
        {key: "val", key2: "val2"},
        {tkey1: "tval1", tkey2: "tval2"}
    );
    stats.flush();

    assert.deepEqual(
        scope.buffer.toString(),
        "series-name,tkey1=tval1,tkey2=tval2 key=val,key2=val2"
    );
    scope.done();
    done();
});

test('should collect a single data point with tags', function(done) {
    var stats = Collector(COLLECTOR_URL);

    var req = nock('http://example.com:8086')
    .filteringRequestBody(/.*/, function(body) {
        return '*';
    })
    .post('/write', '*')
    .query({db: 'test-db', u: 'user', p: 'password', precision: 'ms', database: 'test-db' })
    .reply(200, function(uri, request_body) {
        assert.equal(request_body, 'series-name,host=cat foo=34');
    })

    stats.collect('series-name', { foo: 34.0 }, { host: 'cat' })

    stats.flush();
    setTimeout(function() {
        req.done();
        done();
    }, 100);

});

test('should collect multiple data points', function(done) {
    var stats = Collector(COLLECTOR_URL);

    var req = nock('http://example.com:8086')
    .filteringRequestBody(/.*/, function(body) {
        return '*';
    })
    .post('/write', '*\nseries-name foo=10.2,bar="dog"')
    .query({db: 'test-db', u: 'user', p: 'password', precision: 'ms', database: 'test-db' })
    .reply(200, function(uri, request_body) {
        var expected = 'series-name bar="cat",foo=34\nseries-name foo=10.2,bar="dog"';
        assert.equal(request_body, expected);
    });

    stats.collect('series-name', { bar: 'cat', foo: 34.0 });
    stats.collect('series-name', { foo: 10.2, bar: 'dog' });

    stats.flush();
    setTimeout(function() {
        req.done();
        done();
    }, 100);
});

test('should support instantFlush', function(done) {
    var stats = Collector(COLLECTOR_URL + '?instantFlush=yes');

    var req = nock('http://example.com:8086')
    .filteringRequestBody(/.*/, function(body) {
        return '*';
    })
    .post('/write', '*')
    .query({db: 'test-db', u: 'user', p: 'password', precision: 'ms', database: 'test-db' })
    .reply(200, function(uri, request_body) {
        assert.equal(request_body, 'series-name foo=34');
    });

    stats.collect('series-name', { foo: 34.0 });

    setTimeout(function() {
        req.done();
        done();
    }, 100);
});

test('should support a custom flushInterval', function(done) {
    var stats = Collector(COLLECTOR_URL + '?flushInterval=500');

    var req = nock('http://example.com:8086')
    .filteringRequestBody(/.*/, function(body) {
        return '*';
    })
    .post('/write', '*')
    .query({db: 'test-db', u: 'user', p: 'password', precision: 'ms', database: 'test-db' })
    .reply(200, function(uri, request_body) {
        assert.equal(request_body, 'series foo=34');
    });

    stats.collect('series', { foo: 34.0 })

    setTimeout(function() {
        req.done();
        done();
    }, 1000);
});

test('should support a autoFlush=no', function(done) {
    var stats = Collector(COLLECTOR_URL + '?flushInterval=500&autoFlush=no');

    var req = nock('http://example.com:8086')
    .filteringRequestBody(/.*/, function(body) {
        return '*';
    })
    .post('/write', '*')
    .query({db: 'test-db', u: 'user', p: 'password', precision: 'ms', database: 'test-db' })
    .reply(200, function(uri, request_body) {
        assert.equal(request_body, 'series foo=34');
    });

    stats.collect('series', { foo: 34.0 })

    setTimeout(function() {
        assert(req.isDone() == false);

        stats.flush();

        setTimeout(function() {
            req.done();
            done();
        }, 200);
    }, 1000);
});

test('should support time_precision', function(done) {
    var stats = Collector(COLLECTOR_URL + '?time_precision=s');

    var req = nock('http://example.com:8086')
    .filteringRequestBody(/.*/, function(body) {
        return '*';
    })
    .post('/write', '*')
    .query({db: 'test-db', u: 'user', p: 'password', precision: 's', database: 'test-db' })
    .reply(200, function(uri, request_body) {
        assert.equal(request_body, 'series foo=34 1416512521');
    });

    stats.collect('series', { time: 1416512521, foo: 34.0 });

    stats.flush();
    setTimeout(function() {
        req.done();
        done();
    }, 200);
});
