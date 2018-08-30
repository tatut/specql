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

CREATE MATERIALIZED VIEW company_employees_by_country AS
SELECT c.id, c.name, (e.address).country, COUNT(e.id)
  FROM company c
       JOIN employee e ON e.department IN (SELECT id
                                             FROM department
                                            WHERE "company-id" = c.id)
 WHERE (e.address).country IS NOT NULL
 GROUP BY c.id, c.name, (e.address).country;



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
 bytes bytea NOT NULL,
 tsz TIMESTAMP WITH TIME ZONE NOT NULL
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
        ]::recipient[]),
        ('Advertising list', ARRAY[
        NULL::recipient, ROW('Kekkonen', NULL::address)::recipient]::recipient[]);

--- complex composite

CREATE TYPE inner2 AS (
  foo TEXT
);

CREATE TYPE inner1 AS (
  inners inner2[]
);

CREATE TYPE outerc AS (
  innerc inner1
);

CREATE TABLE outertable (
  id SERIAL PRIMARY KEY,
  outercomposite outerc
);

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


CREATE TABLE underscores (
  foo_bar TEXT,
  something_id INTEGER,
  a_third_column TIMESTAMP
);


-- Testing to-keyword transform inside composite
CREATE TYPE things_that_exist AS ENUM ('chair','lamp','dog','other');
CREATE TYPE thing_inventory AS (
  thing things_that_exist,
  how_many INTEGER
);

CREATE TABLE my_things (
  inventory thing_inventory[]
);




-- Some tables for multiple has-many join tests

-- CREATE TABLE customer (
--   id SERIAL PRIMARY KEY,
--   name TEXT NOT NULL,
--   email TEXT,
--   "shipping-address" address
-- );

-- CREATE TYPE cardtype AS ENUM (
--   'mistercord', 'vasi', 'lunchers league', 'european rapid');

-- CREATE TABLE paymentinfo (
--   type cardtype,
--   "customer-id" INTEGER REFERENCES customer (id),
--   "card-number" VARCHAR(16),
--   "cvv-number" VARCHAR(4),
--   name TEXT,
--   "billing-address" address
-- );

-- CREATE TABLE category (
--   id SERIAL PRIMARY KEY,
--   name TEXT,
--   description TEXT
-- );

-- CREATE TABLE product (
--   id SERIAL PRIMARY KEY,
--   name TEXT,
--   description TEXT
--   -- and so on
-- );

-- -- A many to many link table
-- CREATE TABLE "product-category" (
--   "product-id" INTEGER NOT NULL REFERENCES product (id),
--   "category-id" INTEGER NOT NULL REFERENCES category (id)
-- );


-- CREATE TABLE orderline (
--   "order-id" INTEGER NOT NULL REFERENCES order (id),
--   "product-id" INTEGER NOT NULL REFERENCES product (id),
--   unitprice NUMERIC,
--   quantity INTEGER
-- );

-- CREATE TABLE order (
--   id SERIAL PRIMARY KEY,
--   "ordered-at" DATETIME NOT NULL,
--   "customer-id" INTEGER REFERENCES customer (id)
-- );


--------------------------------
-- Range test types and tables

CREATE TYPE part_price_range AS (
  "part-name" TEXT,
  "part-price-range" int4range
);

CREATE TABLE price_range (
  name TEXT,
  "total-range" int4range,
  "part-price-ranges" part_price_range[]
);

INSERT INTO price_range
       (name, "total-range", "part-price-ranges")
VALUES ('medium systems vehicle',
        '[1000,666000)'::int4range,
	ARRAY[ROW('engine', '(0,10000]'::int4range)::part_price_range,
	      ROW('cargo bay', '[123,99999]'::int4range)::part_price_range]::part_price_range[]);
	      
