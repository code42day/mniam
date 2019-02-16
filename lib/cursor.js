const iterator = require('./iterator');
const trigger = require('./trigger');

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

  async function findInCollection() {
    const collection = await my.collection.open();
    const cursor = collection.find(my.query, my.options);
    cursor.batchSize(my.batchSize).project(my.fields);
    if (my.limit) {
      cursor.limit(my.limit);
    }
    return cursor;
  }

  async function toArray(fn) {
    const cursor = await findInCollection();
    const result = await cursor.toArray();
    cursor.close();
    return trigger(fn, null, result);
  }

  async function eachSeries(onItem, fn) {
    const cursor = await findInCollection();
    iterator.eachSeries(onItem, cursor, fn);
  }

  async function eachLimit(limit, onItem, fn) {
    const cursor = await findInCollection();
    iterator.eachLimit(limit, onItem, cursor, fn);
  }

  return self;
}
