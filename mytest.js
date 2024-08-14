var sqlite = require('sqlite3');
var runner = require('./lib/sqlrunner.js');

console.log('\nOpening SQLite db: ./test.db\n');
const db = new sqlite.Database('./test.db');

var r = new runner.SQLRunner(db, 60)

.setValue('ABOUT_JAMES', "Reads O'Reilly's books.")

 // Create the 3 entity and 2 join tables
.addStmt('TBL_EMP',
         `CREATE TABLE IF NOT EXISTS emp(name TEXT NOT NULL, desc TEXT)`)
.addStmt('TBL_DPT',
         `CREATE TABLE IF NOT EXISTS dept(name TEXT NOT NULL)`)
.addStmt('TBL_TM',
         `CREATE TABLE IF NOT EXISTS toastmaster(name TEXT NOT NULL)`)
.addStmt('TBL_E_D',
         `CREATE TABLE IF NOT EXISTS emp_dept
            (empId INTEGER NOT NULL, deptId INTEGER NOT NULL)`)
.addStmt('TBL_E_TM',
         `CREATE TABLE IF NOT EXISTS emp_tm
            (empId INTEGER NOT NULL, tmId INTEGER NOT NULL)`)

 // Insert 3 entities.
.addStmt('JAMES_ID',
         `INSERT INTO emp(name,desc) VALUES ('James',?)`,
         'TBL_EMP', '$ABOUT_JAMES')
.addStmt('ENG_ID',
         `INSERT INTO dept(name) VALUES ('Engineering')`,
         'TBL_DPT')
.addStmt('CLUB_ID',
         `INSERT INTO toastmaster(name) VALUES ('Talking Heads')`,
         'TBL_TM')

 // Assign James to Engineering and ToastMaster.
.addStmt('EMP_DPT', // only for errors
         `INSERT INTO emp_dept(empId, deptId) VALUES (?,?)`,
         'TBL_E_D', '?JAMES_ID', '?ENG_ID')
.addStmt('EMP_TM', // only for errors
         `INSERT INTO emp_tm(empId, tmId) VALUES (?,?)`,
         'TBL_E_TM', '?JAMES_ID', '?CLUB_ID')

 // let's go!
.start();

console.log('Ok, everything is set up. Just wait.');

