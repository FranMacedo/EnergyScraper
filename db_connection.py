from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
from dotenv import load_dotenv
import os
import logging
load_dotenv()


class Postgresql_connect(object):
    def __init__(self, pgres_host, pgres_port, db_name, ssh, ssh_user, ssh_host, ssh_pkey, psql_user, psql_pass, do_log=False):
        # SSH Tunnel Variables
        self.pgres_host = pgres_host
        self.pgres_port = pgres_port
        self.psql_user = psql_user
        self.psql_pass = psql_pass
        self.db_name = db_name
        self.do_log = do_log
        if ssh == True:
            self.server = SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=ssh_user,
                ssh_private_key=ssh_pkey,
                remote_bind_address=(pgres_host, pgres_port),
            )
            server = self.server
            server.start()  # start ssh server
            self.local_port = server.local_bind_port
            self.print_or_log(f'Server connected via SSH || Local Port: {self.local_port}...')
        elif ssh == False:
            pass

    def print_or_log(self, text):
        if self.do_log:
            logging.info(text)
        else:
            print(text)

    def schemas(self):
        engine = create_engine(
            f'postgresql://{self.psql_user}:{self.psql_pass}@{self.pgres_host}:{self.local_port}/{self.db_name}')
        inspector = inspect(engine)
        self.print_or_log('Postgres database engine inspector created...')
        schemas = inspector.get_schema_names()
        self.schemas_df = pd.DataFrame(schemas, columns=['schema name'])
        self.print_or_log(f"Number of schemas: {len(self.schemas_df)}")
        engine.dispose()
        return self.schemas_df

    def tables(self, schema):
        engine = create_engine(
            f'postgresql://{self.psql_user}:{self.psql_pass}@{self.pgres_host}:{self.local_port}/{self.db_name}')
        inspector = inspect(engine)
        self.print_or_log('Postgres database engine inspector created...')
        tables = inspector.get_table_names(schema=schema)
        self.tables_df = pd.DataFrame(tables, columns=['table name'])
        self.print_or_log(f"Number of tables: {len(self.tables_df)}")
        engine.dispose()
        return self.tables_df

    def query(self, query):
        engine = create_engine(
            f'postgresql://{self.psql_user}:{self.psql_pass}@{self.pgres_host}:{self.local_port}/{self.db_name}')
        self.print_or_log(f'Database [{self.db_name}] session created...')
        self.query_df = pd.read_sql(query, engine)
        self.print_or_log('<> Query Sucessful <>')
        engine.dispose()
        return self.query_df


class GetDB():
    def __init__(self, do_log=False):
        self.pgres = Postgresql_connect(
            pgres_host=os.getenv("P_HOST"),
            pgres_port=int(os.getenv("P_PORT")),
            db_name=os.getenv("DB_NAME"),
            ssh=bool(os.getenv("SSH")),
            ssh_user=os.getenv("SSH_USER"),
            ssh_host=os.getenv("SSH_HOST"),
            ssh_pkey=os.getenv("SSH_KEY"),
            psql_user=os.getenv("P_USER"),
            psql_pass=os.getenv("P_PASS"),
            do_log=do_log
        )

    def loginEdp(self):
        sql_statement = """
        SELECT l.*,
             g.name as gestao,
             i.cpe as cpe
        FROM public.app_loginedp as l
        INNER JOIN app_gestao as g on l.gestao_id = g.id
        LEFT JOIN app_instalacao as i on l.inst_id = i.id
        ;
        """
        return self.pgres.query(query=sql_statement)

    def instalacaoEnergia(self):
        sql_statement = """
        SELECT i.*,
             g.name as gestao,
             t.name as tipo
        FROM public.app_instalacao as i
        INNER JOIN app_gestao as g ON i.gestao_id = g.id
        INNER JOIN app_instalacaotipo as t ON i.tipo_id = t.id
        """
        df = self.pgres.query(query=sql_statement)
        return df.drop(['gestao_id', 'geom', 'tipo_id'], axis=1)


# db = GetDB()
# df = db.instalacaoEnergia()


# # different schemas in db
# df = pgres.schemas()

# # all tables in the schema public
# df = pgres.tables(schema='public')

# # get specific table
# sql_statement = """
# SELECT * FROM public.app_loginedp;
# """
# query_df = pgres.query(query=sql_statement)
# print(query_df)
