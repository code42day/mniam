const iterator = require('./iterator');
const trigger = require('./trigger');

module.exports = aggregate;

function aggregate({ collection }) {
  let self = {
    options,
    pipeline,
    toArray,
    eachLimit,
    eachSeries
  };

  var my = {
    collection,
    options: {},
    pipeline: []
  };

  function pipeline(p) {
    my.pipeline = p;
    return self;
  }

  function options(o) {
    my.options = o;
    return self;
  }

  function pipelineStep(operation, options) {
    var stage = {};
    stage[operation] = options;
    my.pipeline.push(stage);
    return self;
  }

  async function toArray(fn) {
    const c = await my.collection.open();
    const cursor = c.aggregate(my.pipeline, my.options);
    const result = await cursor.toArray();
    return trigger(fn, null, result);
  }

  async function eachSeries(onItem, fn) {
    const c = await my.collection.open();
    const cursor = c.aggregate(my.pipeline, my.options);
    iterator.eachSeries(onItem, cursor, fn);
  }

  async function eachLimit(limit, onItem, fn) {
    const c = await my.collection.open();
    const cursor = c.aggregate(my.pipeline, my.options);
    iterator.eachLimit(limit, onItem, cursor, fn);
  }

  [
    'addFields',
    'bucket',
    'bucketAuto',
    'collStats',
    'count',
    'facet',
    'geoNear',
    'graphLookup',
    'group',
    'indexStats',
    'limit',
    'lookup',
    'match',
    'out',
    'project',
    'redact',
    'replaceRoot',
    'sample',
    'skip',
    'sort',
    'sortByCount',
    'unwind'
  ].forEach(function(name) {
    self[name] = pipelineStep.bind(self, '$' + name);
  });

  return self;
}
