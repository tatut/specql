CREATE TYPE address AS (
  street varchar,
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
VALUES (1, 'R&D');

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
 q quark
);


CREATE VIEW "department-employees" AS
  SELECT d.id,
         d.name,
	 (SELECT COUNT(e.id)
	    FROM employee e
	   WHERE e.department = d.id) AS "employee-count"
    FROM department d;
