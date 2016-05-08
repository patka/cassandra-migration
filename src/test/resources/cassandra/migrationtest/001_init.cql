CREATE TABLE PERSON (id uuid primary key, name varchar);

CREATE INDEX name_index ON PERSON (name);