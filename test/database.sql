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
  company integer REFERENCES company (id),
  name varchar
);


CREATE TABLE employee (
  id serial primary key,
  name varchar NOT NULL,
  title varchar,
  department integer REFERENCES department (id),
  address address
);

-- Insert some data

INSERT
  INTO company (name, "visiting-address", "billing-address")
VALUES ('Acme Inc', '("some street 1", "90120", "FI")'::address, '("other street 42", "424242", "FI")'::address);

INSERT
  INTO department (company, name)
VALUES (1, 'R&D');

INSERT
  INTO employee (name, title, department, address)
VALUES ('Wile E. Coyote', 'Super genious', 1, '(Desert avenue 1,31173,US)'::address),
       ('Max Syöttöpaine', 'über consultant', 1, '(Kujatie 2,90100,FI)'::address);
