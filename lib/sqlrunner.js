var HEARTBEAT   = 500; // 0.5 sec

const WAIT      = 'WAIT';
const STARTED   = 'STARTED';
const ABORTED   = 'ABORTED';
const RESULT    = 'RESULT';
const NO_RESULT = 'NOT_FOUND';
const ERROR     = 'ERROR';
const logPref   = ' ::';

/*
 * The resultName name must be unique throughout the operation.
 */
class StmtDef {
  constructor(resultName, stmt) {
    if (typeof resultName == 'function') { // totally diff process
      this.fxn = resultName;
      // this function will set whatever results or notResults to its heart's content.
    } else {
      if (typeof resultName == 'object') {
        this.resultName = resultName.anyResult;
        this.yesResult  = resultName.yesResult;
      } else {
        this.resultName = resultName;
      }
      this.stmt = stmt.trim().replace('\t', ' ');
      var idx = this.stmt.indexOf(' ');
      if (idx > 0) {
        switch(this.stmt.substring(0, idx).toUpperCase()) {
        case 'INSERT': this.isInsert = true; break;
        case 'SELECT': this.isSelect = true;
                       this.idColumn = getSelectWhat(this.stmt);
                       break;
        }
      }
      idx = this.resultName.indexOf('|');
      if (idx > 0) {
        this.resultDispName = this.resultName.substring(idx+1).trim();
        this.resultName = this.resultName.substring(0,idx).trim();
      }
    }
    this.waitingFor = [];
    this.bindVars   = [];
  }
  rpt(runner) {
//  if (runner && runner.verbose) {
      var args = [ '🎾[' + ts6() + ':' + (this.id||'') + ']' ];
      if (this.resultName) {
        var n = this.resultName;
        if (this.yesResult) n += ' + ' + this.yesResult;
        args.push('{' + n + '}');
      }
      args.push('[' + (this.stmt || 'function') + ']');
      var set = {};
      for (var i=0; i<this.bindVars.length; ++i) {
        var x = this.bindVars[i];
        args.push('<' + x + '>');
        set[x] = 1;
      }
      for (var i=0; i<this.waitingFor.length; ++i) {
        var x = this.waitingFor[i];
        if (!set[x]) args.push('{' + x + '}');
      }
      console.log.apply(console, args);
//  }
    return this;
  }
  dbg(runner) {
    if (runner && runner.verbose) {
      var args = [ '🚦[' + ts6() + ':' + (this.id||'') + '] ' ];
      for (var i=1; i<arguments.length; ++i) args.push(arguments[i]);
      console.log.apply(console, args);
    }
    return this;
  }
  setID(id) { this.id = id; }
  isFunction() { return this.fxn != null; }
  setWaitFor() {
    for (var i in arguments) {
      var name = arguments[i];
      if (name.startsWith('?')) {
        name = name.substring(1);
        this.waitingFor.push(name);
        this.bindVars.push(name);
      } else if (name.startsWith('$')) {
        this.bindVars.push(name);   // retaining the leading $
      } else {
        this.waitingFor.push(name); // name may have prefix '!'
      }
    }
  }
  needNoWait() { return !this.waitingFor.length; }
  checkAndRun(runner) {
    if (this.waitingFor.length) {
      for (var i in this.waitingFor) {
        var wf = this.waitingFor[i];
        var isNeg = wf[0] == '!'; if (isNeg) wf = wf.substring(1);
        var res = runner.getResult(wf);
        switch (res && res.type) {
        case RESULT:
          if (isNeg) {
            this.dbg(runner, '  has RESULT, return ABORTED');
            return ABORTED;
          }
          this.dbg(runner, '  has RESULT, which is good');
          break; // pass
        case NO_RESULT:
          if (isNeg) {
            this.dbg(runner, '  has NO RESULT,', 'which is good');
          } else {
            this.dbg(runner, '  has NO RESULT, return ABORTED');
            return ABORTED;
          }
          break; // pass
        case ERROR:
          this.dbg(runner, '  has ERROR; set error; return ABORTED');
          return ABORTED;
        default:
          return WAIT;
        }
      }
    }
    // Now, all waiting-for have passed. Let's do the work.

    if (this.fxn) {
      this.dbg(runner, '  running function, returning STARTED');
      this.fxn();
      return STARTED;
    }

    var rname = this.resultName, yesname = this.yesResult, dname = this.resultDispName, me = this,
        cb = (err,res) => {
               var val, type;
               if (err) { type = ERROR;  val = err; }
               else     { type = RESULT; val = res; }
               runner.setResult(rname, type, val, dname, me);
             },
        cbID = (err,rows) => {
               var val, type;
               if (err) { type = ERROR; val = err; }
               else
                 switch (rows.length) {
                 case 0:  type = NO_RESULT; break;
                 case 1:  type = RESULT;
                          val = rows[0][me.idColumn];
                          yesname && runner.setResult(yesname, RESULT, val, dname, me);
                          break;
                 default: type = ERROR;
                          val = 'Selecting ID returned ' + len + ' rows; expect a single one.';
                          break;
                 }
               runner.setResult(rname, type, val, dname, me);
             };

    // without bind vars; also no waiting at all.
    if (!this.bindVars.length) {
      if (this.isSelect) {
        this.dbg(runner, '  running SELECT, returning STARTED');
        runner.selectId(this.stmt, cbID);
      } else {
        this.dbg(runner, '  running SQL, returning STARTED');
        runner.dbRunOfParamArray([ runner.db, this.stmt, cb ]);
      }
    }
    else { // with bind vars
      var params = [ this.stmt ];
      for (var i in this.bindVars) {
        var bv = this.bindVars[i];
        if (bv[0] == '$') params.push(runner.getValue(bv.substring(1)));
        else {
          var obj = runner.getResult(bv);
          obj && params.push(obj.value);
        }
      }
      if (this.isSelect)
        params.push(cbID);
      else
        params.push(cb);
      if (this.isSelect) {
        this.dbg(runner, '  running SELECT with', this.bindVars.length, 'bind-vars, returning STARTED');
        runner.selectId.apply(runner, params);
      } else { // general updates
        this.dbg(runner, '  running SQL with',    this.bindVars.length, 'bind-vars, returning STARTED');
        params.unshift(runner.db);
        runner.dbRunOfParamArray(params);
      }
    }
    return STARTED;
  }
}

class SQLRunner {
  constructor(db, timeLimitSecs) {
    this.db = db;
    this.stmtNum = 0;
    this.aVarCnt = 0;
    this.waiting = {};
    this.noWait  = [];
    this.values  = {};
    this.usedNames = {};
    this.idleTimeLimit = timeLimitSecs || 5; // idle time limit
  }
  resetStartTime(first) { this.startTime = new Date().getTime(); if (first) this.origStartTime = this.startTime; }
  setVerbose(yes) { this.verbose = yes; return this; }
  rpt() {
    var args = [ '🎾[' + ts6() + ']' ];
    for (var i=0; i<arguments.length; ++i) args.push(arguments[i]);
    console.log.apply(console, args);
    return this;
  }
  rtRpt() {
    var args = [ '🏓[' + ts6() + ']' ];
    for (var i=0; i<arguments.length; ++i) args.push(arguments[i]);
    console.log.apply(console, args);
    return this;
  }
  dbg() {
    if (this.verbose) {
      var args = [ '🚥[' + ts6() + '] ' ];
      for (var i in arguments) args.push(arguments[i]);
      console.log.apply(console, args);
    }
    return this;
  }
  isStarted() { return this.timer != null; }

  start() {
    this.results = {}; // { name, type:RESULT|NO_RESULT|ERROR, value }
    if (!this.noWait.length) {
      console.error('Strange... no starting SQL statements. Bye!');
      return;
    }
    // start the first batch
    for (var i in this.noWait)
      this.noWait[i].checkAndRun(this);

    // start the wait loop
    this.resetStartTime(true);
    var me = this;
    this.timer = setInterval(() => {
      var a = Object.keys(me.waiting), delCnt = 0, ending = !a.length;
      if (!ending) {
        // try to start those that can
        for (var i in a) {
          var k = a[i],
              stmt = me.waiting[k],
              status = stmt.checkAndRun(me);
          if (status != WAIT) {
            me.dbg(logPref, me.waiting[k].id, 'in', status, 'being removed from waiting queue.');
            delete me.waiting[k];
            ++delCnt;
          }
        }
        if (delCnt) me.resetStartTime();
        ending = (delCnt == a.length);
      }

      // check timeout
      if (!ending) {
        // if all remaining are SELECTs, just quit it.
        ending = true;
        for (var i in me.waiting)
          if (!me.waiting[i].isSelect) { ending = false; break; }
      }
      var dur = (new Date().getTime() - me.origStartTime)/1000;
      if (ending)
        this.rtRpt('All tasks completed in', dur.toFixed(0), 'seconds.');
      else if (me.idleTimeLimit) { // check time limit
        ending = dur > me.idleTimeLimit;
        if (ending) {
          this.rtRpt('Time limit exceeded. The rest of the waiting statements are abandoned:');
          for (var i in me.waiting)
            this.rtRpt(logPref, '['+i+']', me.waiting[i].stmt);
        } else {
          me.rtRpt('Heartbeat:', (me.idleTimeLimit-dur).toFixed(1), 'secs to time out.');
        }
      }
      if (ending) {
        me.rtRpt('Heatbeat: Ends.');
        clearInterval(me.timer);
        me.timer = null;
        return;
      }
      var a = Object.keys(me.waiting), ending = !a.length;
      me.dbg('Time left:', (me.idleTimeLimit-dur).toFixed(0), 'secs:', a.length, 'waiting:', a.join(' '));
    }, HEARTBEAT);
  }

  addCode(fxn) { this.addStmt.apply(this, arguments); return this; }
  addTableExists(tblName, yesName, anyName) {
    this.addSelectId(yesName, anyName,
      `SELECT name FROM sqlite_master WHERE type='table' AND upper(name)='${tblName.toUpperCase()}'`);
    return this;
  }
  addSelectId(yesResultName, anyResultName, stmt) {
    var args = [ { yesResult:yesResultName, anyResult:anyResultName } ];
    for (var i=2; i<arguments.length; ++i) args.push(arguments[i]);
    this.addStmt.apply(this, args);
    return this;
  }
  addStmt(result, stmt) {
    var i = 1;
    if (typeof result == 'function') { // from addCode
      var def = new StmtDef(result);
    } else {
      i = 2;
      var def = new StmtDef(result, stmt);
      var vn;
      if (typeof result == 'object') vn = result.anyResult;
      else if (typeof result == 'string') vn = result;
      if (vn) {
        if (this.usedNames[vn])
          throw 'Result name "' + vn + '" is duplicated for [' + stmt + ']; ' +
                'was added in [' + this.usedNames[vn] + '].';
        this.usedNames[vn] = stmt;
      }
    }

    for (; i<arguments.length; ++i) def.setWaitFor(arguments[i]);
    var id = def.isFunction() ? 'F' : (def.needNoWait() ? 'D' : 'S');
    id += ++this.stmtNum;
    def.setID(id);
    if (!this.isStarted() && def.needNoWait())
      this.noWait.push(def);
    else
      this.waiting[id] = def;
    def.rpt(this);
    return this;
  }
  addInsertIfNotExists(varName, selectStmt, selectParams, insertStmt, insertParams) {
    var avar = this.getAVar(), bvar = this.getAVar(), cvar = this.getAVar();
    // 1. first select
    var args = [ { yesResult:varName||this.getAVar(), anyResult:avar }, selectStmt ];
    if (selectParams) args = args.concat(selectParams);
    this.addStmt.apply(this, args);
    // 2. the insert
    args = [ bvar, insertStmt, '!'+avar ];
    if (insertParams) args = args.concat(insertParams);
    this.addStmt.apply(this, args);
    // 3. second select
    if (varName) {
      args = [ { yesResult:varName, anyResult:cvar }, selectStmt, bvar ];
      if (selectParams) args = args.concat(selectParams);
      this.addStmt.apply(this, args);
    }
  }
  addSeedingSubject(subjName, subjVar) {
    this.addSelectId(subjVar, '#'+subjVar, `SELECT rowid FROM subject WHERE name='${subjName}'`, 'TBL_SUBJ');
    this.addStmt(`##${subjVar}`,
      `INSERT INTO subject(name,createdBy,created,lastUpdate) VALUES ('${subjName}',?,DATE(),DATE())`,
      '!#'+subjVar, '?SYSUSERID');
    this.addSelectId(subjVar, '###'+subjVar, `SELECT rowid FROM subject WHERE name='${subjName}'`, 'TBL_SUBJ', '##'+subjVar);
    return this;
  }
  getAVar() { return ';;;' + (++this.aVarCnt); }
  setValue(name, val) { this.values[name] = val; return this; }
  getValue(name)  { return this.values[name]; }
  getResult(name) { return this.results[name]; }
  setResult(name, type, value, dispName, stmt) {
    if (!name) return;
    var curVal = this.results[name];
    (type == ERROR) && stmt && stmt.rpt(this);
    if (type == RESULT && !value) value = true;
    if (!curVal) {
      this.results[name] = { name, value, type };
      (type == ERROR) && stmt && stmt.rpt(this);
      if (typeof value == 'string') value = '"' + value + '"';
      this.rtRpt(logPref, 'add', type, 'for', dispName||name, (type==NO_RESULT) ? '' : ('= ' + value));
    } else {
      switch(curVal.type) {
      case RESULT:
        curVal.value = value;
        this.rtRpt(logPref, 'set', RESULT, 'for', dispName||name, '=', value);
        break;
      case NO_RESULT:
        switch(type) {
        case RESULT:
        case NO_RESULT: break;
        case ERROR:
          this.rtRpt(logPref, 'set', NO_RESULT, 'for', dispName||name);
          break;
        }
        break;
      case ERROR:
        switch(type) {
        case RESULT:
          curVal.type = RESULT;
          curVal.value = value;
          this.rtRpt(logPref, 'set', RESULT, 'for', dispName||name, '=', value);
          break;
        case NO_RESULT:
          curVal.type = NO_RESULT;
          this.rtRpt(logPref, 'set', NO_RESULT, 'for', dispName||name);
          break;
        case ERROR:
          break;
        }
        break;
      }
    }
  }
  selectId(stmt) { // followed by bind-param values and lastly, possibly a callback
    var params = [ this.db, stmt ], callercb, me = this;
    for (var i=1; i<arguments.length; ++i) {
      var a = arguments[i];
      if (typeof a == 'function') callercb = a;
      else params.push(a);
    }
    if (callercb) {
      params.push((err,data) => { // for SELECT last_insert_rowid()
        if (err) {
          if (me.verbose) console.error(err);
          callercb(err);
        } else {
          callercb(null, data);
        }
      });
    } else {
      params.push((err) => { if (err) console.error(err); });
    }
    this.dbRunOfParamArray(params, 'all');
  }

  dbRunOfParamArray(arr, dbMethod) {
    var db = arr.shift(), sql = arr[0];
    var s = 'Running [' + sql + ']';
    for (var i=1; i<arr.length; ++i) {
      var p = arr[i];
      if (typeof p != 'function') s += ' <' + limitLen(""+p,60) + '>';
    }
    this.rtRpt(s);

    this.db[dbMethod||'run'].apply(this.db, arr);
  }
}

function getSelectWhat(stmt) {
  stmt = stmt.trim().toLowerCase().replace('\t', ' ');
  if (!stmt.startsWith('select ')) return null;
  var idx0 = stmt.indexOf(' ');
  var idx = stmt.indexOf('from');
  return (idx < 0) ? stmt.substring(idx0+1).trim()
                   : stmt.substring(idx0+1,idx).trim();
}

function limitLen(s, lim) {
  if (s.length <= lim) return s;
  return s.substring(0,lim) + '...';
}

function ts6() { var x = new Date().getTime().toString(); return x.substring(x.length-6); }

//============================
// exports
//

module.exports = {
  SQLRunner: SQLRunner
}

