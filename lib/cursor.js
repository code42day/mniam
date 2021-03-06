const async = require('async');
const iterator = require('./iterator');

module.exports = cursor;

function cursor(spec) {
  let self = {
    query,
    fields,
    limit,
    options,
    batchSize,
    toArray,
    eachLimit,
    eachSeries
  };

  let my = {
    collection: spec.collection,
    query: {},
    fields: {},
    options: {},
    batchSize: 100
  };

  function query(q) {
    my.query = q;
    return self;
  }

  function fields(f) {
    my.fields = f;
    return self;
  }

  function options(o) {
    my.options = o;
    return self;
  }

  function batchSize(bs) {
    my.batchSize = bs;
    return self;
  }

  function limit(l) {
    my.limit = l;
    return self;
  }

  function findInCollection(collection, fn) {
    let cursor = collection.find(my.query, my.options);
    cursor.batchSize(my.batchSize).project(my.fields);
    if (my.limit) {
      cursor.limit(my.limit);
    }
    fn(null, cursor);
  }

  function toArray(fn) {
    async.waterfall([
      my.collection.open,
      findInCollection,
      iterator.toArray
    ], fn);
  }

  function eachSeries(onItem, fn) {
    async.waterfall([
      my.collection.open,
      findInCollection,
      async.apply(iterator.eachSeries, onItem)
    ], fn);
  }

  function eachLimit(limit, onItem, fn) {
    async.waterfall([
      my.collection.open,
      findInCollection,
      async.apply(iterator.eachLimit, limit, onItem)
    ], fn);
  }

  return self;
}
