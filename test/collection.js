const should = require('should');
const database = require('../lib/database');
const async = require('async');

/*global describe, it, before, after, beforeEach, afterEach */

describe('collection', function() {
  before(function() {
    this.db = database('mongodb://localhost/mniam-test');
    return this.db.drop();
  });

  after(function() {
    return this.db.drop();
  });

  afterEach(function(done) {
    if (this.collection) {
      this.collection.drop(() => {
        this.collection.close();
        done();
      });
    }
  });

  it('supports crud methods', function(done) {
    const friends = this.db.collection({
      name: 'friends',
      indexes: [[{ name: 1 }]]
    });
    this.collection = friends;

    friends.insertOne({
      name: 'Alice',
      age: 14,
    }, function(err, item) {
      should.not.exist(err);
      should.exist(item);
      item.should.have.property('name', 'Alice');
      item.should.have.property('age', 14);
      item.should.have.property('_id');

      friends.findOneAndUpdate(item._id, {
        $set: { age: 15 },
      }, { returnOriginal: false }, function(err, item) {
        should.not.exist(err);
        should.exist(item);
        item.should.have.property('name', 'Alice');
        item.should.have.property('age', 15);

        friends.deleteOne({ name: 'Alice' }, done);
      });
    });
  });

  it('supports findOneAndReplace', function(done) {
    const friends = this.db.collection({
      name: 'friends',
      indexes: [[{ name: 1 }]]
    });
    this.collection = friends;

    async.waterfall([
      fn => friends.insertOne({ name: 'Alice', age: 14 }, fn),
      (item, fn) => friends.findOneAndReplace({ name: 'Alice'}, { name: 'Bob', age: 33 }, fn),
      (item, fn) => {
        item.should.have.property('name', 'Alice');
        item.should.have.property('age', 14);
        fn();
      },
      fn => friends.findOne({ name: 'Bob'}, fn),
      (item, fn) => {
        item.should.have.property('name', 'Bob');
        item.should.have.property('age', 33);
        fn();
      },
    ], done);
  });

  it('supports findOneAndReplace with options', function(done) {
    const friends = this.db.collection({
      name: 'friends',
      indexes: [[{ name: 1 }]]
    });
    this.collection = friends;

    async.waterfall([
      fn => friends.insertOne({ name: 'Alice', age: 14 }, fn),
      (item, fn) => friends.findOneAndReplace(
        { name: 'Alice'},
        { name: 'Bob', age: 33 },
        { returnOriginal: false },
        fn
      ),
      (item, fn) => {
        item.should.have.property('name', 'Bob');
        item.should.have.property('age', 33);
        fn();
      },
    ], done);
  });

  it('supports findOneAndDelete', function(done) {
    const friends = this.db.collection({
      name: 'friends',
      indexes: [[{ name: 1 }]]
    });
    this.collection = friends;

    async.waterfall([
      fn => friends.insertOne({ name: 'Alice', age: 14 }, fn),
      (item, fn) => friends.findOneAndDelete({ name: 'Alice'}, fn),
      (item, fn) => {
        item.should.have.property('name', 'Alice');
        item.should.have.property('age', 14);
        fn();
      },
      fn => friends.findOne({ name: 'Alice'}, err => {
        should.exist(err);
        fn();
      })
    ], done);
  });

  it('findOneAndUpdate accepts query as argument', function(done) {
    const friends = this.db.collection({
      name: 'friends',
      indexes: [[{ name: 1 }]]
    });
    this.collection = friends;

    async.waterfall([
      function(fn) {
        friends.insertOne({name: 'Bob', age: 33 }, fn);
      },
      function(item, fn) {
        friends.findOneAndUpdate({ name: 'Bob' }, { $set: {age: 34 } }, fn);
      },
      function(item, fn) {
        item.should.have.property('name', 'Bob');
        item.should.have.property('age', 34);
        item.should.have.property('_id');

        fn();
      }
    ], done);
  });


  it('drop removes all items from collection', function(done) {
    const values = this.db.collection({
      name: 'values',
    });
    this.collection = values;

    async.series([
      function(fn) {
        async.times(10, function(i, fn) {
          values.insertOne({ value: `x${i}` }, fn);
        }, fn);
      },
      function(fn) {
        values.find({}, {}, {}, function(err, items) {
          items.should.have.length(10);
          fn(err);
        });
      },
      function(fn) {
        values.drop(fn);
      },
      function(fn) {
        values.find({}, {}, {}, function(err, items) {
          items.should.have.length(0);

          fn(err);
        });
      }
    ], done);
  });

  describe('should insert', function() {

    before(function() {
      this.friends = this.db.collection({
        name: 'friends',
        indexes: [[{ name: 1 }]]
      });
    });

    after(function(done) {
      this.friends.drop(() => {
        this.friends.close();
        done();
      });
    });

    beforeEach(function(done) {
      this.friends.removeMany({}, done);
    });

    it('one', function(done) {
      const { friends } = this;

      async.waterfall([
        function(fn) {
          friends.insertOne({name: 'Bob', age: 34 }, fn);
        },
        function(item, fn) {
          friends.query({ name: 'Bob' }).toArray(fn);
        },
        function(items, fn) {
          items.should.have.length(1);
          let item = items[0];
          item.should.have.property('name', 'Bob');
          item.should.have.property('age', 34);
          item.should.have.property('_id');

          friends.close();
          fn();
        }
      ], done);
    });

    it('many', function(done) {
      const { friends } = this;

      async.waterfall([
        function(fn) {
          friends.insertMany([
            {name: 'Bob', age: 33 },
            {name: 'Alice', age: 20 },
            {name: 'Cyril', age: 21 }
          ], fn);
        },
        function(item, fn) {
          friends.query({ name: 'Bob' }).toArray(fn);
        },
        function(items, fn) {
          items.should.have.length(1);
          let item = items[0];
          item.should.have.property('name', 'Bob');
          item.should.have.property('age', 33);
          item.should.have.property('_id');
          fn();
        },
        function(fn) {
          friends.query({ name: 'Alice' }).toArray(fn);
        },
        function(items, fn) {
          items.should.have.length(1);
          let item = items[0];
          item.should.have.property('name', 'Alice');
          item.should.have.property('age', 20);
          item.should.have.property('_id');
          fn();
        }
      ], done);
    });
  });

  describe('query', function() {
    const TEST_LEN = 421;

    before(function() {
      this.numbers = this.db.collection({
        name: 'numbers',
        batchSize: 33
      });
    });

    after(function(done) {
      this.numbers.drop(() => {
        this.numbers.close();
        done();
      });
    });

    beforeEach(function(done) {
      const { numbers } = this;

      function send(value, fn) {
        numbers.insertOne({ value }, fn);
      }

      async.times(TEST_LEN, send, done);
    });

    afterEach(function(done) {
      this.numbers.removeMany({}, done);
    });

    it('eachLimit iterates over all elements of collection', function(done) {
      const { numbers } = this;
      const results = [];

      function receive(item, fn) {
        results[item.value] = true;
        async.setImmediate(fn);
      }

      numbers.eachLimit(7, receive, function(err) {
        // need all items received
        results.should.have.length(TEST_LEN);
        // and they all need to be true
        results.filter(function(x) { return x; }).should.have.length(TEST_LEN);

        done(err);
      });
    });

    it('find elements by query', function(done) {
      const { numbers } = this;
      const results = [];

      function receive(item, fn) {
        results.push({ value: item.value });
        async.setImmediate(fn);
      }

      numbers
      .query({ value: 10 })
      .eachSeries(receive, function(err) {
        results.should.have.length(1);
        results[0].should.eql({ value: 10 });

        done(err);
      });
    });

    it('find elements by query with fields and options', function(done) {
      const { numbers } = this;

      numbers
      .query({ value: { $lt: 10, $gte: 5 } })
      .fields({ _id: 0 })
      .options({
        limit: 3,
        sort: { value: -1 }
      })
      .toArray(function(err, results) {
        results.should.have.length(3);
        results[0].should.eql({ value: 9 });
        results[1].should.eql({ value: 8 });
        results[2].should.eql({ value: 7 });
        done(err);
      });
    });
  });

  describe('bulk', function() {
    before(function() {
      this.items = this.db.collection({ name: 'items' });
    });

    after(function(done) {
      this.items.drop(() => {
        this.items.close();
        done();
      });
    });

    beforeEach(function(done) {
      const { items } = this;

      function send(value, fn) {
        items.insertOne({ _id: value }, fn);
      }

      async.times(10, send, done);
    });

    afterEach(function(done) {
      this.items.drop(done);
    });

    it('updates multiple documents', function(done) {
      this.items.bulkWrite([
        { updateOne: { filter: { _id:1 }, update: { $set: { name: 'a' } } } },
        { updateOne: { filter: { _id:2 }, update: { $set: { name: 'b' } } } },
        { updateOne: { filter: { _id:3 }, update: { $set: { name: 'c' } } } }
      ], function(err, results) {
        results.nModified.should.eql(3);
        done();
      });
    });
  });
});
