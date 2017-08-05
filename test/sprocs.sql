-- Test stored procedure definition

CREATE TYPE "issuetype-stats" AS (
  type issuetype,
  percentage NUMERIC(5,2)
);

-#-

CREATE OR REPLACE FUNCTION "calculate-issuetype-stats" (statuses status[], containing TEXT)
RETURNS SETOF "issuetype-stats" AS
$$
DECLARE
  r RECORD;
  total INTEGER;
  percentage NUMERIC(5,2);
BEGIN
  SELECT INTO total COUNT(*)
    FROM issue i
   WHERE i.status = ANY(statuses)
     AND i.title LIKE ('%'||containing||'%');

  FOR r IN SELECT t.type, COUNT(i.id) FILTER (WHERE i.status = ANY(statuses)
                                                AND i.title LIKE ('%'||containing||'%')) as issues
             FROM (SELECT unnest(enum_range(NULL::issuetype)) AS type) t
	          LEFT JOIN issue i ON i.type = t.type
	    GROUP BY t.type
  LOOP
    IF total = 0 THEN
      percentage = 0;
    ELSE
      percentage = r.issues * 100.0 / total;
    END IF;
    RETURN NEXT (r.type, percentage)::"issuetype-stats";
  END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION "calculate-issuetype-stats" (statuses status[], containing TEXT)
IS 'Calculate the percentages of issues for each type. Considers issues that have any of the given statuses and contain the given text.';


CREATE OR REPLACE FUNCTION myrange (from_ INT, to_ INT) RETURNS INT[] AS $$
DECLARE
  arr INT[];
  i INT;
BEGIN
  i := from_;
  LOOP
    IF i = to_ THEN
      EXIT;
    END IF;
    arr := arr || i;
    i := i + 1;
  END LOOP;
  RETURN arr;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION myrange (from_ INT, to_ INT)
IS 'Returns an array of successive integers in the range from_ (inclusive) -- to_ (exclusive).'
