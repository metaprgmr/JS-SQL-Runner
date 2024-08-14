# JS SQL Runner

The asynchronous nature of NodeJS makes running SQL tedious and verbose. This little utility sports serially running SQL statements without complicated asynchronous programming. It can pass on the newly inserted row IDs, and serialize the execution of SQL statements for dependent operations.

All the use cases are covered by the test script, <code>mytest.js</code>. The current version is written and tested against SQLite and MySQL, but can and should be adapted to any RDBMS's. The only thing to be customized is the way to get the last INSERTed ID, which is not covered by standard SQL.

Set up your favourite NodeJS environment, make sure SQLite3 module is installed, then run <code>mytest.js</code>, and you shall see its output as follows:

<pre>
% node mytest.js 

Opening SQLite db: ./test.db

Running [CREATE TABLE IF NOT EXISTS emp(name TEXT NOT NULL, desc TEXT)]
Running [CREATE TABLE IF NOT EXISTS dept(name TEXT NOT NULL)]
Running [CREATE TABLE IF NOT EXISTS toastmaster(name TEXT NOT NULL)]
Running [CREATE TABLE IF NOT EXISTS emp_dept
            (empId INTEGER NOT NULL, deptId INTEGER NOT NULL)]
Running [CREATE TABLE IF NOT EXISTS emp_tm
            (empId INTEGER NOT NULL, tmId INTEGER NOT NULL)]
Ok, everything is set up. Just wait.
Running [INSERT INTO emp(name,desc) VALUES ('James',?)] &lt;Reads O'Reilly's books.>
Running [INSERT INTO dept(name) VALUES ('Engineering')]
Running [INSERT INTO toastmaster(name) VALUES ('Talking Heads')]
Running [INSERT INTO emp_dept(empId, deptId) VALUES (?,?)] <1> <1>
Running [INSERT INTO emp_tm(empId, tmId) VALUES (?,?)] <1> <1>

All tasks completed in 1.008 seconds.
</pre>
