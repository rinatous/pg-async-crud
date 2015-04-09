

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

DROP DATABASE asyncpg;
CREATE DATABASE asyncpg WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'ru_RU.UTF-8' LC_CTYPE = 'ru_RU.UTF-8';


\connect asyncpg

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

CREATE SCHEMA public;

SET search_path = public, pg_catalog;

CREATE TABLE table1 (
    id integer NOT NULL,
    text1 character varying
);

CREATE SEQUENCE table1_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE table1_id_seq OWNED BY table1.id;

ALTER TABLE ONLY table1 ALTER COLUMN id SET DEFAULT nextval('table1_id_seq'::regclass);

ALTER TABLE ONLY table1
    ADD CONSTRAINT table1_pkey PRIMARY KEY (id);
