const mongo = require('mongodb');
const debug = require('debug')('mniam');

const trigger = require('./trigger');

module.exports = database;

/**
 * Lazily creates database connections: takes the same parameters as MongoClient.connect
 *
 * @param url {String} mongo DB URL
 * @param opts {Object} hash of server, db and replSet options overwriting defaults from DB URL
 */
function database(url, opts) {
  let self = {
    open,
    close,
    collection,
    drop,
    objectID: require('./object-id')  // deprecated - use mniam.objectID
  };

  opts = Object.assign({}, opts, { useNewUrlParser: true });

  let openCount = 0;
  let openPromise;
  let client;

  async function doOpen() {
    debug('Connecting: %s', url);
    client = await mongo.MongoClient.connect(url, opts);
    return client.db();
  }

  async function open() {
    openCount += 1;
    debug('Opening DB...', openCount);
    if(!openPromise) {
      openPromise = doOpen();
    }
    return openPromise;
  }

  function close() {
    if (!openPromise) {
      return;
    }
    openCount -= 1;
    debug('Closing DB...', openCount);
    if(openCount < 1) {
      openPromise = undefined;
      client.close();
      client = undefined;
    }
  }

  function collection(spec) {
    spec.db = self;
    return require('./collection')(spec);
  }

  async function drop(fn) {
    try {
      const db = await open();
      await db.dropDatabase();
      close();
      return trigger(fn);
    } catch(e) {
      return trigger(fn, e);
    }
  }

  return self;
}
