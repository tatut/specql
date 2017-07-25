CREATE TYPE address AS (
  street varchar(20),
  "postal-code" varchar,
  country varchar
);


CREATE TABLE company (
  id serial primary key,
  name varchar NOT NULL,
  "visiting-address" address,
  "billing-address" address
);

CREATE TABLE department (
  id serial primary key,
  "company-id" integer REFERENCES company (id),
  name varchar
);

CREATE TABLE project (
  id serial primary key,
  "department-id" integer REFERENCES department (id),
  name text
  -- this table is just for testing joins
);

CREATE TABLE deliverable (
  "project-id" integer REFERENCES project (id),
  name text
);

CREATE TABLE employee (
  id serial primary key,
  name varchar NOT NULL,
  title varchar,
  department integer REFERENCES department (id),
  address address,
  "employment-started" DATE NOT NULL,
  "employment-ended" DATE
);

-- Insert some data

INSERT
  INTO company (name, "visiting-address", "billing-address")
VALUES ('Acme Inc', '("some street 1","90120","FI")'::address, '("other street 42","424242","FI")'::address),
       ('Omni Consumer Products', '(kujatie 1,90999,FI)'::address, NULL);

INSERT
  INTO department ("company-id", name)
VALUES (1, 'R&D'), (2, 'Marketing');

INSERT
  INTO employee (name, title, department, address, "employment-started", "employment-ended")
VALUES ('Wile E. Coyote', 'Super genious', 1, '(Desert avenue 1,31173,US)'::address, '1949-09-17'::date, NULL),
       ('Max Syöttöpaine', 'über consultant', 1, '(Kujatie 2,90100,FI)'::address, '2017-01-01'::date, NULL),
       ('Foo Barsky', 'metasyntactic checker', 1, NULL, '2010-07-07'::date, '2016-12-31'::date);


CREATE TYPE quark AS ENUM ('up', 'down','strange','charm','bottom','top');


CREATE TABLE typetest (
 int integer NOT NULL,
 numeric numeric NOT NULL,
 text text NOT NULL,
 date date NOT NULL,
 bool bool NOT NULL,
 q quark NOT NULL,
 ts TIMESTAMP NOT NULL,
 uuid uuid NOT NULL,
 bytes bytea NOT NULL
);


CREATE VIEW "department-employees" AS
  SELECT d.id,
         d.name,
	 (SELECT COUNT(e.id)
	    FROM employee e
	   WHERE e.department = d.id) AS "employee-count"
    FROM department d;

-- silly table to test joins
CREATE TABLE "department-meeting" (
 id SERIAL PRIMARY KEY,
 "start-time" TIMESTAMP,
 "end-time" TIMESTAMP,
 "subject" TEXT,
 "department1-id" INTEGER NOT NULL REFERENCES department (id),
 "department2-id" INTEGER NOT NULL REFERENCES department (id)
);

CREATE TABLE "department-meeting-notes" (
  "department-meeting-id" INTEGER REFERENCES "department-meeting" (id),
  time TIMESTAMP,
  note TEXT
);

-- For testing upsert!
CREATE UNIQUE INDEX ON "department-meeting-notes" ("department-meeting-id", time);

INSERT INTO "department-meeting"
       ("start-time", "end-time", "subject", "department1-id", "department2-id")
VALUES ('2017-03-07T09:00:00', '2017-03-07T11:00:00',
        'ad campaigns for new widgets',
	1, 2);

INSERT INTO "department-meeting-notes"
       ("department-meeting-id", time, note)
VALUES (1, '2017-03-07T09:01:00', 'Rolf suggested a new campaign called: widgets4all'),
       (1, '2017-03-07T09:02:00', 'Max seconded the idea, but asked for cost estimates'),
       (1, '2017-03-07T09:45:00', 'After lengthy dicussion, it was decided that RFPs would be sent to the usual ad agencies');


--- Test array and composite type

CREATE TYPE recipient AS (
  name varchar,
  address address
);

CREATE TABLE mailinglist (
  name varchar,
  recipients recipient[]
);

INSERT INTO mailinglist (name, recipients)
VALUES ('Fake News Quarterly',
        ARRAY[
         ROW('Max Syöttöpaine', ROW('Kujatie 1','90100','FI')::address)::recipient,
         ROW('Erno Penttikoski', ROW('Tiekuja 3','90666','FI')::address)::recipient,
	 ROW('Henna Lindberg', ROW('Kujakuja 5','4242','FI')::address)::recipient
        ]::recipient[]);



-- Test name/type clashes

-- Specing these tables in the same namespace should give error about
-- two incompatible specs for the start keyword.
CREATE TABLE typeclash1 ( start TIMESTAMP );
CREATE TABLE typeclash2 ( start TIME );

-- Specing these tables in the same namespace should give error about
-- nameclash1 keyword already referring to a table
CREATE TABLE nameclash1 ( foo integer );
CREATE TABLE nameclash2 ( nameclash1 varchar );


-- Test field transformation

CREATE TYPE status AS ENUM ('open','in-progress','resolved');

CREATE TYPE issuetype AS ENUM ('bug','feature');
CREATE TABLE issue (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT,
  status status,
  type issuetype
);
