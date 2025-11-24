--
-- PostgreSQL database cluster dump
--

\restrict rM1HJRueBacIbwfpQD7Z8aHkgzc4pQyzqgatFgaIOAJx3khSQRVP7LhBjzUnFzj

SET default_transaction_read_only = off;

SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

--
-- Roles
--

CREATE ROLE postgres;
ALTER ROLE postgres WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS;
CREATE ROLE xtl;
ALTER ROLE xtl WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION NOBYPASSRLS PASSWORD 'SCRAM-SHA-256$4096:ozfxxKKvKFTVQpSMlcc40g==$2qdJH/gJgtqTJaoxEyJGa+zQdrC6GaVV+gb+l9ORFb0=:/bSBZaCKV7rpVxpm0YhMKnJ0z8qCmitR8M8Rb37QK+o=';






\unrestrict rM1HJRueBacIbwfpQD7Z8aHkgzc4pQyzqgatFgaIOAJx3khSQRVP7LhBjzUnFzj

--
-- PostgreSQL database cluster dump complete
--

