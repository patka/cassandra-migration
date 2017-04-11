CREATE TABLE EVENT (id uuid primary key, name varchar);

CREATE INDEX name_index ON EVENT (name);