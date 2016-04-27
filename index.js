
var sqlite3 = require('sqlite3');
var lodash  = require('lodash');
var async   = require('async');

// just open the DB...
function openDatabase(cb) {
  console.log('openDatabase...');
  var db = new sqlite3.Database(__dirname+'/sqlite-thing.db', function (err) {
    if (err) return cb(err);
    
    // strongly recommended for debugging SQLITE
    db.on('trace', function (item) {
      console.log('TRACE: ', item);
    });
    
    cb(null, db);
  });
}

// initialize the DB...
function initDatabase(db, cb) {
  console.log('initDatabase...');
  async.series([
    function (next) {
      db.exec('PRAGMA foreign_keys=0;', next);
    },
    function (next) {
      db.exec('BEGIN TRANSACTION;', next);
    },
    function (next) {
      db.exec(
        'CREATE TABLE IF NOT EXISTS thing ('+
        ' id INTEGER PRIMARY KEY AUTOINCREMENT,'+
        ' name TEXT NOT NULL'+
        ');',
        next
      );
    },
    
    // (add more tables...)
    
    function (next) {
      db.exec('COMMIT;', next);
    },
    function (next) {
      db.exec('PRAGMA foreign_keys=1;', next);
    }
  ], function (err) {
    if (err) return cb(err);
    cb(null, db);
  });
}

// define the operations allowed for the DB...
function prepareStatements(db, cb) {
  console.log('prepareStatements...');
  
  function prepareStatement(sql, next) {
    var stmt = db.prepare(sql, function (err) {
      if (err) return next(err);
      next(null, stmt);
    });
  }
  
  async.series({
    stmtRetrieve: function (next) {
      console.log('prepare stmtRetrieve...');
      prepareStatement('SELECT * FROM thing', next);
    },
    stmtCreate: function (next) {
      console.log('prepare stmtCreate...');
      prepareStatement('INSERT INTO thing (name) VALUES ($name)', next);
    },
    stmtRetrieveLastRowId: function (next) {
      console.log('prepare stmtRetrieveLastRowId...');
      prepareStatement('SELECT last_insert_rowid() AS last_row_id', next);
    },
  }, function (err, statements) {
    if (err) return cb(err);
    
    var dbAdapter = {
      db:                    db,
      stmtRetrieve:          statements.stmtRetrieve,
      stmtCreate:            statements.stmtCreate,
      stmtRetrieveLastRowId: statements.stmtRetrieveLastRowId
    };
    
    cb(null, dbAdapter);
  });
  
}

// operation for adding new things and retrieve their ids in a transaction
function addThing (dbAdapter, name, cb) {
  console.log('add...');
  
  // can mix statements and prepared statements...
  async.series([
    function (next) {
      dbAdapter.db.exec('BEGIN TRANSACTION;', next);
    },
    function (next) {
      dbAdapter.stmtCreate.run(name, next);
    },
    function (next) {
      dbAdapter.stmtRetrieveLastRowId.get(next);
    },
    function (next) {
      dbAdapter.db.exec('COMMIT;', next);
    },
  ], function (err, results) {
    if (err) return cb(err); // ROLLBACK?
    
    // don't use reset methods asynchronously here to prevent segmentation fault
    dbAdapter.stmtCreate.reset();
    dbAdapter.stmtRetrieveLastRowId.reset();
    
    cb(null, results[2].last_row_id);
  });
}

// operation for getting a list of all things
function retrieveThings (dbAdapter, cb) {
  console.log('retrieve...');
  dbAdapter.stmtRetrieve.all(function (err, results) {
    if (err) return cb(err);
    
    dbAdapter.stmtRetrieve.reset();
    
    cb(null, results);
  });
}

// startup...
async.waterfall([
  openDatabase,
  initDatabase,
  prepareStatements
], function (err, dbAdapter) {
  if (err) {
    console.log(err);
    console.log(err.stack);
    return;
  }
  
  // start batch...
  async.series({
    
    // create two things and retrieve their ids...
    ids: function (next) {
      async.mapSeries(['hello', 'world'], addThing.bind(null, dbAdapter), function (err, ids) {
        if (err) return next(err);
        next(null, ids);
      });
    },
    
    // retrieve all things...
    things: function (next) {
      retrieveThings(dbAdapter, function (err, things) {
        if (err) return next(err);
        next(null, things);  
      }); 
    }
    
  }, function (err, results) {
    if (err) {
      console.log(err);
      console.log(err.stack);
      return;
    }
    
    // present results...
    console.log('created two things with ids', results.ids.join(', '));
    
    // use lodash for synchronous operations on collections...
    lodash.each(results.things, function (thing) {
      console.log(thing);
    });
    
  });
  
});
