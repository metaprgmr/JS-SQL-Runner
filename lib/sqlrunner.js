var HEARTBEAT = 500; // 0.5 sec

class StmtDef {
  constructor(resultName, stmt) {
    this.stmt = stmt.trim();
    this.isInsert = this.stmt.toUpperCase().startsWith('INSERT ');
    this.resultName = resultName || 'NONAME';
    this.waitingFor = [];
    this.bindVars = [];
  }
  setWaitFor() {
    for (var i in arguments) {
      var name = arguments[i];
      if (name.startsWith('?')) {
        name = name.substring(1);
        this.waitingFor.push(name);
        this.bindVars.push(name);
      } else if (name.startsWith('$')) {
        this.bindVars.push(name); // retaining the leading $
      } else {
        this.waitingFor.push(name);
      }
    }
  }
  needNoWait() { return !this.waitingFor.length; }
  checkAndRun(runner) {
    if (this.waitingFor.length)
      for (var i in this.waitingFor)
        if (!runner.results[this.waitingFor[i]]) return false;

    var rname = this.resultName,
        cb = (err,res) => {
               if (err) runner.setError (rname, err);
               else     runner.setResult(rname, res);
             };

    // without bind vars; also no waiting at all.
    if (!this.bindVars.length) {
      if (this.isInsert)
        runner.insertReturnID(this.stmt, cb);
      else // general updates
        dbRunOfParamArray([ runner.db, this.stmt, cb ]);
      return true;
    }

    // with bind vars
    var params = [ runner.db, this.stmt ];
    for (var i in this.bindVars) {
      var bv = this.bindVars[i];
      if (bv[0] == '$') params.push(runner.getValue(bv.substring(1)));
      else              params.push(runner.getResult(bv));
    }
    params.push(cb);
    if (this.isInsert) {
      params.shift();
      runner.insertReturnID.apply(runner, params);
    } else // general updates
      dbRunOfParamArray(params);
    return true;
  }
}

class SQLRunner {
  constructor(db, timeLimitSecs, dbtype) {
    this.db = db;
    this.dbType = dbtype || 'SQLite';
    this.stmtNum = 0;
    this.waiting = {};
    this.noWait  = [];
    this.values  = {};
    this.timeLimit = timeLimitSecs || 60 * 30; // default half an hour
  }

  start() {
    this.results = {};
    this.hasErrors = false;
    if (!this.noWait.length) {
      console.error('Strange... no starting SQL statements. Bye!');
      return;
    }
    // start the first batch
    for (var i in this.noWait)
      this.noWait[i].checkAndRun(this);

    // start the wait loop
    this.startTime = new Date().getTime();
    var me = this;
    this.timer = setInterval(() => {
      var started = [],
          a = Object.keys(me.waiting),
          ending = !a.length;
      
      // try to start those that can
      for (var i in a)
        if (me.waiting[a[i]].checkAndRun(me)) started.push(a[i]);
      if (started.length == a.length) { // Wow! All done!
        me.waiting = {}; // why not
        ending = true;
      }
      else { // remove the started ones from the waiting list
        for (var i in started) {
          delete me.waiting[started[i]];
        }
      }

      // check timeout
      var dur = (new Date().getTime() - me.startTime)/1000;
      if (ending)
        console.log('\nAll tasks completed in', dur, 'seconds.\n');
      else if (me.timeLimit) { // check time limit
        if (dur > me.timeLimit) ending = true;
        if (ending)
          console.log('Time limit exceeded. The rest of the waiting statements are aborted.');
      }
      if (ending) {
        clearInterval(me.timer);
        me.timer = null;
      }
    }, HEARTBEAT);
  }

  addStmt(resultName, stmt) {
    var def = new StmtDef(resultName, stmt);
    for (var i=2; i<arguments.length; ++i) def.setWaitFor(arguments[i]);
    if (def.needNoWait()) this.noWait.push(def);
    else this.waiting['S' + (++this.stmtNum)] = def;
    return this;
  }
  setValue(name, val) { this.values[name] = val; return this; }
  getValue(name)  { return this.values[name]; }
  getResult(name) { return this.results[name]; }
  setResult(name, value) { this.results[name] = value || true; }
  setError (name, err)   { this.hasErrors = true; console.error(name+' has failed--', err); }
  insertReturnID(stmt) { // followed by bind-param values and lastly, possibly a callback
    var params = [ this.db, stmt ], callercb;
    for (var i=1; i<arguments.length; ++i) {
      var a = arguments[i];
      if (typeof a == 'function') callercb = a;
      else params.push(a);
    }
    if (callercb) {
      params.push((err,data) => { // for SELECT last_insert_rowid()
        if (err) callercb(err);
        else { // get the new rowid
            var lastIdCall = this.getLastInsertIdSQL(stmt);
            this.db.all(lastIdCall.select, (err1,rows) => {
            if (err1) callercb(err1);
            else if (rows && rows.length) callercb(null, rows[0][lastIdCall.name]);
          });
        }
      });
    } else {
      params.push((err) => { if (err) console.error(err); });
    }

    dbRunOfParamArray(params);
  }
  getLastInsertIdSQL(stmt) {
    // This is the impl for SQLite. Modify for your type of RDBMS
    switch(this.dbType.toUpperCase()) {
    case 'SQLITE':
      return { select:'SELECT last_insert_rowid() FROM ' + getInsertTableName(stmt),
               name:  'last_insert_rowid()'
             };
    case 'MYSQL':
      return { select:'SELECT last_insert_id()', name:'last_insert_id()' };
    default:
      throw 'SQLRunner TODO: override getLastInsertIdSQL() for ' + this.dbType + '.';
    }
  }
}

function dbRunOfParamArray(arr) {
  var db = arr.shift(), sql = arr[0];
  var s = 'Running [' + sql + ']';
  for (var i=1; i<arr.length; ++i) {
    var p = arr[i];
    if (typeof p != 'function') s += ' <' + p + '>';
  }
  console.log(s);

  db.run.apply(db, arr);
}

function getInsertTableName(stmt) {
  // sniff out the table name
  stmt = stmt.trim().toLowerCase();
  if (!stmt.startsWith('insert ')) return null;
  var i, idx = stmt.indexOf(' into ');
  idx += 6;
  var limit = stmt.length-8; // sauf VALUES(), at least
  for (i=idx; i<limit; ++i) {
    var c = stmt[i];
    if (c == ' ' || c == '(') break;
  }
  var ret = stmt.substring(idx, i).trim();
  return ret;
}

//============================
// exports
//

module.exports = {
  SQLRunner: SQLRunner,
  StmtDef:   StmtDef
}

