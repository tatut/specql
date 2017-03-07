# specql: easy PostgreSQL queryies

specql makes querying PostgreSQL databases easy by introspecting tables at compile time and creating
specs for the rows and columns. Specql can then automatically do CRUD operations on the tables as
the table structure is known.

specql uses namespaced keys for all tables and columns and makes it easy to spot
errors at development time.
