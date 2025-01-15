SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: errors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.errors (
    task_id character varying(128) NOT NULL,
    name character varying(128) NOT NULL,
    message character varying NOT NULL,
    cause character varying,
    stacktrace character varying,
    retries_left smallint NOT NULL,
    created_at timestamp with time zone NOT NULL
);


--
-- Name: results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.results (
    task_id character varying(128) NOT NULL,
    result character varying,
    created_at timestamp with time zone NOT NULL
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying(128) NOT NULL
);


--
-- Name: task_parents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_parents (
    task_id character varying(128) NOT NULL,
    parent_id character varying(128) NOT NULL,
    provided_argument character varying(128) NOT NULL
);


--
-- Name: tasks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tasks (
    id character varying(128) NOT NULL,
    name character varying(128) NOT NULL,
    group_id character varying(128),
    state character varying(16) NOT NULL,
    progress real,
    created_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    retries_left smallint NOT NULL,
    max_retries smallint,
    args character varying
);


--
-- Name: results results_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.results
    ADD CONSTRAINT results_pkey PRIMARY KEY (task_id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: tasks tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_pkey PRIMARY KEY (id);


--
-- Name: index_errors_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_errors_created_at ON public.errors USING btree (created_at);


--
-- Name: index_results_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_results_created_at ON public.results USING btree (created_at);


--
-- Name: index_tasks_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_tasks_created_at ON public.tasks USING btree (created_at);


--
-- Name: index_tasks_state; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX index_tasks_state ON public.tasks USING btree (state);


--
-- Name: errors errors_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.errors
    ADD CONSTRAINT errors_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.tasks(id);


--
-- Name: results results_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.results
    ADD CONSTRAINT results_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.tasks(id);


--
-- Name: task_parents task_parents_parent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_parents
    ADD CONSTRAINT task_parents_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.tasks(id);


--
-- Name: task_parents task_parents_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_parents
    ADD CONSTRAINT task_parents_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.tasks(id);


--
-- PostgreSQL database dump complete
--


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20240802104314'),
    ('20240827142011'),
    ('20240919114022'),
    ('20241108104336'),
    ('20250114171324'),
    ('20250115170559');
