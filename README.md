# RDBS

Tables.txt  - data structure
Data.txt    - the data itself
app.py      - ORM

## SELECT commands

```sql
-- Average count
WITH TableCounts AS (
    SELECT 'Authors' AS table_name, COUNT(*) AS row_count FROM Authors
    UNION ALL SELECT 'Genres', COUNT(*) FROM Genres
    UNION ALL SELECT 'Members', COUNT(*) FROM Members
    UNION ALL SELECT 'Positions', COUNT(*) FROM Positions
    UNION ALL SELECT 'Employees', COUNT(*) FROM Employees
    UNION ALL SELECT 'Items', COUNT(*) FROM Items
    UNION ALL SELECT 'Items_Authors', COUNT(*) FROM Items_Authors
    UNION ALL SELECT 'Items_Genres', COUNT(*) FROM Items_Genres
    UNION ALL SELECT 'Labels', COUNT(*) FROM Labels
    UNION ALL SELECT 'Labels_Members_Employees', COUNT(*) FROM Labels_Members_Employees
)
SELECT 
    SUM(row_count) AS total_records,
    COUNT(*) AS total_tables,
    AVG(row_count) AS average_records_per_table
FROM TableCounts;
```

```sql
-- Embedded SELECT - total number of loans shows only above average count
SELECT 
    m.id_member,
    m.first_name,
    m.last_name,
    COUNT(lme.id_loan) AS total_loans
FROM 
    Members m
JOIN 
    Labels_Members_Employees lme ON m.id_member = lme.id_member
GROUP BY 
    m.id_member, m.first_name, m.last_name
HAVING 
    COUNT(lme.id_loan) > (
        SELECT AVG(cnt)
        FROM (
            SELECT COUNT(*) AS cnt
            FROM Labels_Members_Employees
            GROUP BY id_member
        ) sub
    );
```

```sql
--Analytical function DENSE_RANK
SELECT 
    m.first_name,
    m.last_name,
    COUNT(lme.id_loan) AS total_loans,
    DENSE_RANK() OVER (ORDER BY COUNT(lme.id_loan) DESC) AS loan_rank
FROM 
    Members m
LEFT JOIN 
    Labels_Members_Employees lme ON m.id_member = lme.id_member
GROUP BY 
    m.id_member, m.first_name, m.last_name;
```

```sql
--Recursive SELECT only recusive table
WITH RECURSIVE PositionHierarchy AS (
    -- 1. Anchor Member
    SELECT 
        id_pos, 
        title, 
        salary, 
        id_manager, 
        1 AS level,
        CAST(title AS TEXT) AS path -- Tracks the path for visualization
    FROM 
        Positions
    WHERE 
        id_manager IS NULL

    UNION ALL

    -- 2. Recursive Member
    SELECT 
        p.id_pos, 
        p.title, 
        p.salary, 
        p.id_manager, 
        ph.level + 1,
        CAST(ph.path || ' -> ' || p.title AS TEXT)
    FROM 
        Positions p
    INNER JOIN 
        PositionHierarchy ph ON p.id_manager = ph.id_pos
)
-- 3. Final Select: Output the built hierarchy
SELECT 
    level,
    id_pos,
    title,
    salary,
    path
FROM 
    PositionHierarchy
ORDER BY 
    path;
```

## VIEW

```sql
--Info about all books including genres and authors
CREATE OR REPLACE VIEW View_Book_Details AS
SELECT 
    i.title AS book_title,
    i.type,
    i.publisher,
    i.language,
    i.code,
    i.pages,
    a.first_name AS author_first_name,
    a.last_name AS author_last_name,
    g.genre_name
FROM 
    Items i
-- Join Items to Authors via the bridge table
JOIN 
    Items_Authors ia ON i.id_item = ia.id_item
JOIN 
    Authors a ON ia.id_author = a.id_author
-- Join Items to Genres via the bridge table
JOIN 
    Items_Genres ig ON i.id_item = ig.id_item
JOIN 
    Genres g ON ig.id_genre = g.id_genre;

SELECT * FROM View_Book_Details;
```

## INDEX

```sql
-- Ensures that the combination of First Name + Last Name + Birthdate is unique
CREATE UNIQUE INDEX idx_unique_author_identity 
ON Authors (first_name, last_name, birthdate);

INSERT INTO Authors (id_author, first_name, last_name, birthdate, nationality)
VALUES (52,'George', 'Orwell', '1903-06-25', 'British');

-- Should throw an error
INSERT INTO Authors (id_author, first_name, last_name, birthdate, nationality)
VALUES (53,'George', 'Orwell', '1903-06-25', 'British');
```

Fulltext search

```sql
-- Use before and after creating the index
EXPLAIN ANALYZE
SELECT id_item, title, publisher
FROM Items 
WHERE to_tsvector('english', title || ' ' || COALESCE(publisher, '')) 
      @@ to_tsquery('english', 'son');

-- Index both Title and Publisher as one searchable document
CREATE INDEX idx_fulltext_items_combined 
ON Items 
USING GIN (
    to_tsvector('english', title || ' ' || COALESCE(publisher, ''))
);
```

## FUNCTION

```sql
-- Function to search via mail
CREATE OR REPLACE FUNCTION get_unclosed_loan_count(p_identifier VARCHAR) 
RETURNS INTEGER AS $$
DECLARE
    loan_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO loan_count
    FROM Labels_Members_Employees lme
    JOIN Members m ON lme.id_member = m.id_member
    WHERE 
        -- Check if the input matches the Email
        (m.email = p_identifier)
        -- An unclosed loan is defined by a NULL returned_date
        AND lme.returned_date IS NULL;
      
    RETURN loan_count;
END;
$$ LANGUAGE plpgsql;

-- Function to search via id
CREATE OR REPLACE FUNCTION get_unclosed_loan_count(p_identifier INTEGER) 
RETURNS INTEGER AS $$
DECLARE
    loan_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO loan_count
    FROM Labels_Members_Employees lme
    JOIN Members m ON lme.id_member = m.id_member
    WHERE 
        (m.id_member = p_identifier)
        -- An unclosed loan is defined by a NULL returned_date
        AND lme.returned_date IS NULL;
      
    RETURN loan_count;
END;
$$ LANGUAGE plpgsql;

SELECT get_unclosed_loan_count('cmartinez@example.com');

SELECT get_unclosed_loan_count(1);
```

## PROCEDURE

Find members with overdue loans and simulate notifing them by raising a notice

```sql
CREATE OR REPLACE PROCEDURE notify_overdue_members()
LANGUAGE plpgsql
AS $$
DECLARE
    -- 1. Declare variables to hold cursor data
    v_member_id INTEGER;
    v_first_name VARCHAR(100);
    v_email VARCHAR(255);
    v_due_date DATE;
    
    -- 2. Declare the Cursor
    -- Selects members who have an unclosed loan past the due date
    cur_overdue CURSOR FOR 
        SELECT m.id_member, m.first_name, m.email, lme.due_date
        FROM Labels_Members_Employees lme
        JOIN Members m ON lme.id_member = m.id_member
        WHERE lme.returned_date IS NULL 
          AND lme.due_date < CURRENT_DATE;

BEGIN
    -- Open the cursor
    OPEN cur_overdue;

    LOOP
        -- Fetch the next row into variables
        FETCH cur_overdue INTO v_member_id, v_first_name, v_email, v_due_date;
        
        -- Exit the loop when no more rows are found
        EXIT WHEN NOT FOUND;

        -- 3. Error Handling Block (Nested Block)
        BEGIN
            -- Check for specific business rule error: Missing Email
            IF v_email IS NULL THEN
                RAISE EXCEPTION 'Member % (ID: %) has no email address defined.', v_first_name, v_member_id;
            END IF;

            -- Simulate sending an email (Should be INSERT into a notifications table)
            RAISE NOTICE 'Sending email to % (%) about loan due on %', v_first_name, v_email, v_due_date;

        EXCEPTION
            -- Catch the specific error raised above (or any other unexpected error)
            WHEN OTHERS THEN
                -- Log the error to the console but DO NOT crash the whole procedure.
                -- This allows the loop to continue to the next member.
                RAISE NOTICE 'SKIPPING: Could not process Member ID %: %', v_member_id, SQLERRM;
        END;
    END LOOP;

    -- Close the cursor
    CLOSE cur_overdue;
END;
$$;
-- More after implementing log trigger
CALL notify_overdue_members();

-- similar select
SELECT * FROM labels_members_employees
WHERE returned_date IS NULL AND due_date < CURRENT_DATE;
```

## TRIGGER

```sql
--Logs table
CREATE TABLE Logs (
    id_log SERIAL PRIMARY KEY,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    db_user VARCHAR(100) DEFAULT CURRENT_USER,
    action_type VARCHAR(10),  -- 'INSERT', 'UPDATE', or 'DELETE'
    table_name VARCHAR(100),
    data_old JSONB,           -- Snapshot of the row BEFORE change
    data_new JSONB            -- Snapshot of the row AFTER change
);

-- Trigger itself
CREATE OR REPLACE FUNCTION log_activity()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO Logs (action_type, table_name, data_new)
        VALUES ('INSERT', TG_TABLE_NAME, to_jsonb(NEW));
        RETURN NEW;
        
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO Logs (action_type, table_name, data_old, data_new)
        VALUES ('UPDATE', TG_TABLE_NAME, to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
        
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO Logs (action_type, table_name, data_old)
        VALUES ('DELETE', TG_TABLE_NAME, to_jsonb(OLD));
        RETURN OLD;
    END IF;
    
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Attaching
CREATE TRIGGER trg_audit_members
AFTER INSERT OR UPDATE OR DELETE ON Members
FOR EACH ROW
EXECUTE FUNCTION log_activity();

--Testing
CALL notify_overdue_members();
UPDATE Members 
SET email = NULL
WHERE id_member = 98;
CALL notify_overdue_members();
UPDATE Members 
SET email = 'kathy37@example.com'
WHERE id_member = 98;

SELECT * FROM Logs;
```

## TRANSACTION

```sql
CREATE OR REPLACE PROCEDURE safe_give_raise(
    p_pos_id INTEGER, 
    p_percent_increase DECIMAL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_new_salary NUMERIC;
    v_max_limit CONSTANT NUMERIC := 100000.00; -- The safety cap
BEGIN
    -- 1. Perform the update
    UPDATE Positions
    SET salary = salary * (1 + p_percent_increase)
    WHERE id_pos = p_pos_id
    RETURNING salary INTO v_new_salary;

    -- 2. Check validation logic
    IF v_new_salary > v_max_limit THEN
        -- CONDITION FAILED: Undo the update
        ROLLBACK;
        RAISE NOTICE 'Transaction Rolled Back: New salary % exceeds the limit of %', v_new_salary, v_max_limit;
    ELSE
        -- CONDITION MET: Save the update
        COMMIT;
        RAISE NOTICE 'Transaction Committed: Salary updated to %', v_new_salary;
    END IF;
END;
$$;

-- Scenario 1: Small raise (Successful Commit)
CALL safe_give_raise(3, 0.05); 

-- Scenario 2: Huge raise (Triggers Rollback)
CALL safe_give_raise(10, 1.00);
```

## USER

```sql
CREATE ROLE analyst;

-- Grant read-only access
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;

-- Create user
CREATE USER clara WITH PASSWORD 'clara123';

-- Assign role
GRANT analyst TO clara;

------
-- Change to clara in Welcome window

SELECT * FROM Members;
TRUNCATE Members;
------

-- Lock
BEGIN;
LOCK TABLE Members IN SHARE MODE NOWAIT;

------
-- As Clara
-- Doesn't go through
UPDATE Members 
SET email = NULL
WHERE id_member = 98;

-- Goes through (SHARE MODE)
SELECT * FROM Members;
------

-- Unlock
COMMIT;

REVOKE analyst FROM clara;
DROP USER clara;
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM analyst;
DROP ROLE analyst;
```