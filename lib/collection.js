const util = require('util');
const debug = require('debug')('mniam');

const aggregateCursor = require('./aggregate');
const cursor = require('./cursor');
const trigger = require('./trigger');

module.exports = collection;

function normalizeIndexSpec(indexes) {
  return indexes.map(([ key, options ]) => Object.assign({ key }, options));
}

/**
 * Creates a collection object for the database
 *
 * @param spec.name name of the collection
 * @param spec.indexes array of tuples specifying Mongo index as a tuple { fields,
 *          options }
 * @param spec.db database
 *
 * @returns collection object
 */

function collection({ db, name, indexes = [] }) {
  let self = {
    aggregate,
    bulkWrite,
    findOne,
    findOneAndDelete,
    findOneAndReplace,
    findOneAndUpdate,
    insertOne,
    insertMany,
    deleteOne,
    deleteMany,
    updateOne,
    updateMany,
    replaceOne,
    query,
    indexInformation,
    open,
    close,
    drop,
    options: setOptions,
    find,
    forEach,
    eachLimit,
    // deprecated aliases
    removeOne: deleteOne,
    removeMany: deleteMany,
  };

  indexes = normalizeIndexSpec(indexes);
  let options = {};
  const batchSize = 100;
  let promiseCollection;

  async function doOpen() {
    debug('Opening collection %s', name);
    const mongoDb = await db.open();
    const c = await mongoDb.collection(name);
    if (indexes.length > 0) {
      debug('Ensuring %d indexes', indexes.length);
      await c.createIndexes(indexes);
    }
    return c;
  }

  async function open(fn) {
    if(!promiseCollection) {
      promiseCollection = doOpen();
    }
    let mongoCollection = await promiseCollection;
    return trigger(fn, null, mongoCollection);
  }

  function close() {
    if (!promiseCollection) {
      debug('Ignoring close for %s', name);
      return;
    }
    debug('Closing collection %s', name);
    promiseCollection = undefined;
    db.close();
  }

  function query(q = {}) {
    return cursor({ collection: self }).query(q);
  }

  function aggregate(pipeline = [], options = {}) {
    return aggregateCursor({ collection: self }).pipeline(pipeline).options(options);
  }

  function find(q, fields, options, fn) {
    debug('find %j %j %j', query, fields, options);
    query(q).fields(fields).options(options).batchSize(batchSize).toArray(fn);
  }

  function forEach(onItem, fn) {
    debug('forEach');
    query().batchSize(batchSize).eachSeries(onItem, fn);
  }

  // similar to forEach but processes items in packets up to `limit` size
  function eachLimit(limit, onItem, fn) {
    debug('each with limit %d', limit);
    query().batchSize(batchSize).eachLimit(limit, onItem, fn);
  }

  async function findOneAndDelete(query, options, fn) {
    [ options, fn ] = optionsArgs( options, fn );
    debug('findOneAndDelete %j', query);

    try {
      const c = await open();
      const result = await c.findOneAndDelete(query, options);
      const { value } = result || {};
      return trigger(fn, null, value);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function findOneAndReplace(query, replacement, options, fn) {
    [ options, fn ] = optionsArgs( options, fn );
    debug('findOneAndReplace %j %j %j', query, replacement, options);
    try {
      const c = await open();
      const result = await c.findOneAndReplace(query, replacement, options);
      debug('Result %j', result);
      const { value } = result || {};
      return trigger(fn, null, value);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function findOneAndUpdate(query, update, options, fn) {
    [ options, fn ] = optionsArgs( options, fn );
    debug('findOneAndUpdate %j %j', query, update);
    options = Object.assign({
      returnOriginal: false
    }, options);
    try {
      const c = await open();
      const result = await c.findOneAndUpdate(query, update, options);
      const { value } = result || {};
      return trigger(fn, null, value);
    } catch (e) {
      trigger(fn, e);
    }
  }

  function findOne(q, fn) {
    debug('findOne %j', q);

    query(q).limit(1).toArray(function(err, items) {
      if(err) { return fn(err); }
      err = items.length > 0 ? null : 'no items found' + util.inspect(query);
      fn(err, items[0]);
    });
  }

  function setOptions(o) {
    Object.assign(options, o);
    return self;
  }

  async function insertOne(doc, fn) {
    try {
      const c = await open();
      const { ops } = await c.insertOne(doc, options);
      return trigger(fn, null, ops[0]);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function insertMany(docs, fn) {
    try {
      const c = await open();
      const { ops } = await c.insertMany(docs, options);
      return trigger(fn, null, ops);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function deleteOne(filter, fn) {
    try {
      const c = await open();
      const result = await c.deleteOne(filter, options);
      return trigger(fn, null, result);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function deleteMany(filter, fn) {
    try {
      const c = await open();
      const result = await c.deleteMany(filter, options);
      return trigger(fn, null, result);
    } catch (e) {
      trigger(fn, e);
    }
  }

  function optionsArgs(o, fn) {
    if (typeof o === 'function') {
      return [ options, o ];
    }
    return [ Object.assign({}, options, o) , fn ];
  }

  async function updateOne(filter, data, options, fn) {
    [ options, fn ] = optionsArgs( options, fn );
    try {
      const c = await open();
      const result = await c.updateOne(filter, data, options);
      return trigger(fn, null, result);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function updateMany(filter, data, options, fn) {
    [ options, fn ] = optionsArgs( options, fn );
    try {
      const c = await open();
      const result = await c.updateMany(filter, data, options);
      return trigger(fn, null, result);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function replaceOne(filter, data, options, fn) {
    [ options, fn ] = optionsArgs( options, fn );
    debug('replaceOne %j %j', filter, data);
    try {
      const c = await open();
      const result = await c.replaceOne(filter, data, options);
      return trigger(fn, null, result);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function bulkWrite(operations, options, fn) {
    [ options, fn ] = optionsArgs( options, fn );
    debug('bulkWrite %d', operations.length);
    try {
      const c = await open();
      const result = await c.bulkWrite(operations, options);
      return trigger(fn, null, result);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function drop(fn) {
    debug('drop %s', name);
    try {
      const c = await open();
      const result = await c.drop();
      return trigger(fn, null, result);
    } catch (e) {
      trigger(fn, e);
    }
  }

  async function indexInformation(fn) {
    try {
      const c = await open();
      const result = await c.indexInformation();
      return trigger(fn, null, result);
    } catch (e) {
      trigger(fn, e);
    }
  }

  return self;
}
