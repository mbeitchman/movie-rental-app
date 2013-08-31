CREATE TABLE Plan(pid INT PRIMARY KEY, name VARCHAR(50), maxrental INT, monthlyfee MONEY);
CREATE TABLE Customer(cid INT PRIMARY KEY, login VARCHAR(30), password VARCHAR(30), fname VARCHAR(50), lname VARCHAR(50), planid INT REFERENCES Plan(pid));
CREATE TABLE Rental(customerid INT REFERENCES Customer(cid), status BOOL, movieid INT, checkoutime TIMESTAMP);

INSERT INTO Plan VALUES(0,'Basic', 1, '5');
INSERT INTO Plan VALUES(1,'Plus', 3, '10');
INSERT INTO Plan VALUES(2,'Super', 5, '13');
INSERT INTO Plan VALUES(3,'Ulimate', 10, '17');

INSERT INTO Customer VALUES(0,'marc', 'marc', 'marc','beitchman', 3);
INSERT INTO Customer VALUES(1,'john', '1234QQ','john','doe', 1);
INSERT INTO Customer VALUES(2,'billg', 'PPo89', 'bill', 'gates', 0);
INSERT INTO Customer VALUES(3,'algre876', '987yu', 'al', 'green', 2);
INSERT INTO Customer VALUES(4,'billcli', 'fox12', 'bill', 'clinton', 1);
INSERT INTO Customer VALUES(5,'rachel', 'rachel', 'rachel', 'greene', 3);
INSERT INTO Customer VALUES(6,'alice', '1357alice', 'alice', 'waters', 2);
INSERT INTO Customer VALUES(7,'bob', '2593bob', 'bob', 'johnson', 0);

INSERT INTO Rental VALUES(0,TRUE, 130321, TIMESTAMP '2011-10-01 15:00:00.000000 EST');
INSERT INTO Rental VALUES(1,FALSE, 130321, TIMESTAMP '2011-10-01 15:00:00.000000 EST');
INSERT INTO Rental VALUES(2,TRUE, 12209, TIMESTAMP '2011-10-02 13:00:00.000000 EST');
INSERT INTO Rental VALUES(3,FALSE, 1008, TIMESTAMP '2011-9-01 10:00:00.000000 EST');
INSERT INTO Rental VALUES(4,FALSE, 280, TIMESTAMP '2011-9-10 09:00:00.000000 EST');
INSERT INTO Rental VALUES(5,TRUE, 7778, TIMESTAMP '2011-10-18 07:00:00.000000 EST');
INSERT INTO Rental VALUES(6,TRUE, 10003, TIMESTAMP '2011-10-16 18:00:00.000000 EST');
INSERT INTO Rental VALUES(7,TRUE, 10033, TIMESTAMP '2011-10-04 22:00:00.000000 EST');