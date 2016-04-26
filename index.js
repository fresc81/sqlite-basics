
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
  
  async.series([
    function (next) {
      console.log('prepare stmtRetrieve...');
      prepareStatement('SELECT * FROM thing', next);
    },
    function (next) {
      console.log('prepare stmtCreate...');
      prepareStatement('INSERT INTO thing (name) VALUES ($name)', next);
    },
    function (next) {
      console.log('prepare stmtRetrieveLastRowId...');
      prepareStatement('SELECT last_insert_rowid() AS last_row_id', next);
    },
  ], function (err, statements) {
    if (err) return cb(err);
    
    var dbAdapter = {
      db: db,
      stmtRetrieve: statements[0],
      stmtCreate: statements[1],
      stmtRetrieveLastRowId: statements[2]
    };
    
    cb(null, dbAdapter);
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
  
  // operation for adding new things and retrieve their ids in a transaction
  function add (name, cb) {
    console.log('add...');
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
  function retrieve (cb) {
    console.log('retrieve...');
    dbAdapter.stmtRetrieve.all(function (err, results) {
      if (err) return cb(err);
      
      dbAdapter.stmtRetrieve.reset();
      
      cb(null, results);
    });
  }
  
  // create a thing named hello...
  add('hello', function (err, id) {
    if (err) {
      console.log(err);
      console.log(err.stack);
      return;
    }
    console.log('created thing with id ', id);
    
    // create a second thing named world (must be run in serial, not in parallel)...
    add('world', function (err, id) {
      if (err) {
        console.log(err);
        console.log(err.stack);
        return;
      }
      console.log('created thing with id ', id);
      
      // retrieve the list of things...
      retrieve(function (err, things) {
        if (err) {
          console.log(err);
          console.log(err.stack);
          return;
        }
        
        // use lodash for synchronous operations on collections...
        lodash.each(things, function (thing) {
          console.log(thing);
        });
        
      }); 
    });
    
  });
  
});
