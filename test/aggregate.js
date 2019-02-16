const database = require('../lib/database');

/*global describe, it, before, after, beforeEach, afterEach */

describe('aggregate', function() {
  before(function() {
    this.db = database('mongodb://localhost/mniam-test');
    return this.db.drop();
  });

  after(function() {
    return this.db.drop();
  });

  beforeEach(function(done) {
    this.collection = this.db.collection({ name: 'books' });
    this.collection.insertOne({
      title : 'this is my title',
      author : 'bob',
      pageViews : 5,
      tags : [ 'fun' , 'good' , 'fun' ],
      other : { foo : 5 },
    }, done);
  });

  afterEach(function() {
    this.collection.close();
  });

  it('should process pipeline', function(done) {
    this.collection
      .aggregate()
      .project({ author: 1, tags: 1 })
      .unwind('$tags')
      .group({
        _id : { tags : '$tags' },
        authors : { $addToSet : '$author' },
        count: { $sum: 1 }
      })
      .sort({ count: -1 })
      .toArray(function(err, results) {
        results.should.eql([
          { _id: { 'tags': 'fun' }, 'authors': [ 'bob' ], count: 2 },
          { _id: { 'tags': 'good' }, 'authors': [ 'bob' ], count: 1 }
        ]);
        done(err);
      });
  });
});
