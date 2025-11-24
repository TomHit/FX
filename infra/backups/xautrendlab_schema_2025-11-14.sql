--
-- PostgreSQL database dump
--

\restrict 6ApKVwM2ik95hKFfycnRoyXyvOEz6kJqkIJzJRhZMYPGMwsstVvJCsAVkGy6T2U

-- Dumped from database version 14.19 (Ubuntu 14.19-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 14.19 (Ubuntu 14.19-0ubuntu0.22.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: citext; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;


--
-- Name: EXTENSION citext; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION citext IS 'data type for case-insensitive character strings';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: auth_identities; Type: TABLE; Schema: public; Owner: xtl
--

CREATE TABLE public.auth_identities (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    provider text NOT NULL,
    subject text NOT NULL,
    email public.citext,
    email_verified boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.auth_identities OWNER TO xtl;

--
-- Name: device_claims; Type: TABLE; Schema: public; Owner: xtl
--

CREATE TABLE public.device_claims (
    device_id text NOT NULL,
    token text NOT NULL,
    code text NOT NULL,
    status text NOT NULL,
    user_id text,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.device_claims OWNER TO xtl;

--
-- Name: devices; Type: TABLE; Schema: public; Owner: xtl
--

CREATE TABLE public.devices (
    id text NOT NULL,
    user_id text,
    name text,
    status text DEFAULT 'pending'::text NOT NULL,
    device_token text,
    pair_code text,
    pair_expires_at timestamp with time zone,
    last_heartbeat_at timestamp with time zone,
    mt5_ok boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.devices OWNER TO xtl;

--
-- Name: pages; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.pages (
    id text NOT NULL,
    label text NOT NULL
);


ALTER TABLE public.pages OWNER TO postgres;

--
-- Name: role_page_perms; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.role_page_perms (
    role_id uuid NOT NULL,
    page_id text NOT NULL,
    can_view boolean DEFAULT false NOT NULL,
    can_write boolean DEFAULT false NOT NULL
);


ALTER TABLE public.role_page_perms OWNER TO postgres;

--
-- Name: roles; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.roles (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.roles OWNER TO postgres;

--
-- Name: user_download_tokens; Type: TABLE; Schema: public; Owner: xtl
--

CREATE TABLE public.user_download_tokens (
    token text NOT NULL,
    user_id text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    consumed_at timestamp with time zone,
    device_id text,
    zip_password text
);


ALTER TABLE public.user_download_tokens OWNER TO xtl;

--
-- Name: user_page_overrides; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.user_page_overrides (
    user_id uuid NOT NULL,
    page_id text NOT NULL,
    can_view boolean,
    can_write boolean
);


ALTER TABLE public.user_page_overrides OWNER TO postgres;

--
-- Name: users; Type: TABLE; Schema: public; Owner: xtl
--

CREATE TABLE public.users (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    username public.citext NOT NULL,
    email public.citext NOT NULL,
    password_hash text NOT NULL,
    role text DEFAULT 'user'::text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    must_change_password boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_login_at timestamp with time zone,
    mfa_enabled boolean DEFAULT false NOT NULL,
    mfa_secret bytea,
    mfa_temp_secret text,
    mfa_temp_set_at timestamp with time zone,
    CONSTRAINT users_role_check CHECK ((role = ANY (ARRAY['user'::text, 'admin'::text]))),
    CONSTRAINT users_status_check CHECK ((status = ANY (ARRAY['active'::text, 'suspended'::text, 'pending'::text]))),
    CONSTRAINT users_username_check CHECK ((username OPERATOR(public.~) '^[a-z][a-z0-9._-]{2,31}$'::public.citext))
);


ALTER TABLE public.users OWNER TO xtl;

--
-- Name: auth_identities auth_identities_pkey; Type: CONSTRAINT; Schema: public; Owner: xtl
--

ALTER TABLE ONLY public.auth_identities
    ADD CONSTRAINT auth_identities_pkey PRIMARY KEY (id);


--
-- Name: auth_identities auth_identities_provider_subject_key; Type: CONSTRAINT; Schema: public; Owner: xtl
--

ALTER TABLE ONLY public.auth_identities
    ADD CONSTRAINT auth_identities_provider_subject_key UNIQUE (provider, subject);


--
-- Name: device_claims device_claims_pkey; Type: CONSTRAINT; Schema: public; Owner: xtl
--

ALTER TABLE ONLY public.device_claims
    ADD CONSTRAINT device_claims_pkey PRIMARY KEY (device_id);


--
-- Name: devices devices_pkey; Type: CONSTRAINT; Schema: public; Owner: xtl
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_pkey PRIMARY KEY (id);


--
-- Name: pages pages_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pages
    ADD CONSTRAINT pages_pkey PRIMARY KEY (id);


--
-- Name: role_page_perms role_page_perms_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.role_page_perms
    ADD CONSTRAINT role_page_perms_pkey PRIMARY KEY (role_id, page_id);


--
-- Name: roles roles_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_name_key UNIQUE (name);


--
-- Name: roles roles_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_pkey PRIMARY KEY (id);


--
-- Name: user_download_tokens user_download_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: xtl
--

ALTER TABLE ONLY public.user_download_tokens
    ADD CONSTRAINT user_download_tokens_pkey PRIMARY KEY (token);


--
-- Name: user_page_overrides user_page_overrides_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_page_overrides
    ADD CONSTRAINT user_page_overrides_pkey PRIMARY KEY (user_id, page_id);


--
-- Name: users users_email_key; Type: CONSTRAINT; Schema: public; Owner: xtl
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: xtl
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_username_key; Type: CONSTRAINT; Schema: public; Owner: xtl
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_username_key UNIQUE (username);


--
-- Name: device_claims_code_idx; Type: INDEX; Schema: public; Owner: xtl
--

CREATE INDEX device_claims_code_idx ON public.device_claims USING btree (code);


--
-- Name: device_claims_token_idx; Type: INDEX; Schema: public; Owner: xtl
--

CREATE INDEX device_claims_token_idx ON public.device_claims USING btree (token);


--
-- Name: idx_authident_user; Type: INDEX; Schema: public; Owner: xtl
--

CREATE INDEX idx_authident_user ON public.auth_identities USING btree (user_id);


--
-- Name: idx_devices_last_hb; Type: INDEX; Schema: public; Owner: xtl
--

CREATE INDEX idx_devices_last_hb ON public.devices USING btree (last_heartbeat_at);


--
-- Name: idx_devices_token; Type: INDEX; Schema: public; Owner: xtl
--

CREATE INDEX idx_devices_token ON public.devices USING btree (device_token);


--
-- Name: idx_devices_user_id; Type: INDEX; Schema: public; Owner: xtl
--

CREATE INDEX idx_devices_user_id ON public.devices USING btree (user_id);


--
-- Name: idx_udt_user_expires; Type: INDEX; Schema: public; Owner: xtl
--

CREATE INDEX idx_udt_user_expires ON public.user_download_tokens USING btree (user_id, expires_at);


--
-- Name: udt_token_idx; Type: INDEX; Schema: public; Owner: xtl
--

CREATE INDEX udt_token_idx ON public.user_download_tokens USING btree (token);


--
-- Name: uq_devices_pair_code; Type: INDEX; Schema: public; Owner: xtl
--

CREATE UNIQUE INDEX uq_devices_pair_code ON public.devices USING btree (pair_code) WHERE (pair_code IS NOT NULL);


--
-- Name: auth_identities auth_identities_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: xtl
--

ALTER TABLE ONLY public.auth_identities
    ADD CONSTRAINT auth_identities_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: role_page_perms role_page_perms_page_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.role_page_perms
    ADD CONSTRAINT role_page_perms_page_id_fkey FOREIGN KEY (page_id) REFERENCES public.pages(id) ON DELETE CASCADE;


--
-- Name: role_page_perms role_page_perms_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.role_page_perms
    ADD CONSTRAINT role_page_perms_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id) ON DELETE CASCADE;


--
-- Name: user_page_overrides user_page_overrides_page_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_page_overrides
    ADD CONSTRAINT user_page_overrides_page_id_fkey FOREIGN KEY (page_id) REFERENCES public.pages(id) ON DELETE CASCADE;


--
-- Name: user_page_overrides user_page_overrides_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_page_overrides
    ADD CONSTRAINT user_page_overrides_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: xtl
--

REVOKE ALL ON SCHEMA public FROM postgres;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO xtl;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- Name: TABLE pages; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.pages TO xtl;


--
-- Name: TABLE role_page_perms; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.role_page_perms TO xtl;


--
-- Name: TABLE roles; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.roles TO xtl;


--
-- Name: TABLE user_page_overrides; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.user_page_overrides TO xtl;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES  TO xtl;


--
-- PostgreSQL database dump complete
--

\unrestrict 6ApKVwM2ik95hKFfycnRoyXyvOEz6kJqkIJzJRhZMYPGMwsstVvJCsAVkGy6T2U

