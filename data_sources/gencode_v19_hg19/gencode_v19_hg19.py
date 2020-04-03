from sqlalchemy import MetaData, Table, select
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import Selectable, func
from typing import Optional, List
from data_sources.io_parameters import Vocabulary, Mutation, GenomicInterval
from data_sources.annot_interface import AnnotInterface
import database.database as database
import database.db_utils as utils
from threading import RLock
from loguru import logger

table_name = 'hg19_gencode_v19_red'
table_schema = 'rr'
initializing_lock = RLock()
ann_table: Optional[Table] = None
db_meta: MetaData


class GencodeV19HG19(AnnotInterface):

    log_sql_statements: bool = True
    # MAP ATTRIBUTE NAMES TO TABLE COLUMN NAMES
    col_map = {
        Vocabulary.CHROM: 'chrom',
        Vocabulary.START: 'start',
        Vocabulary.STOP: 'stop',
        Vocabulary.STRAND: 'strand',
        Vocabulary.GENE_NAME: 'gene_name',
        Vocabulary.GENE_TYPE: 'gene_type'
    }

    def __init__(self):
        self.connection: Optional[Connection] = None
        self.init_singleton_table()

    def annotate(self, connection: Connection, genomic_interval: GenomicInterval, attrs: Optional[List[Vocabulary]]) -> Selectable:
        """
        :param connection:
        :param genomic_interval:
        :param attrs: a list of Vocabulary elements indicating the kind of annotation attributes desired
        :return: a statement that when executed returns the annotation data requested.
        """
        self.connection = connection
        columns_of_interest = [ann_table.c[self.col_map[attr]].label(attr.name) for attr in attrs]
        stmt = \
            select(columns_of_interest) \
            .where((ann_table.c.start <= genomic_interval.stop) &
                   (ann_table.c.stop >= genomic_interval.start) &
                   (ann_table.c.chrom == genomic_interval.chrom))
        if genomic_interval.strand is not None and genomic_interval.strand != 0:
            stmt = stmt.where(ann_table.c.strand == genomic_interval.strand)
        if self.log_sql_statements:
            utils.show_stmt(connection, stmt, logger.debug, 'ANNOTATE REGION/VARIANT')
        return stmt

    def find_gene_region(self, connection: Connection, gene_name: str, gene_type: Optional[str]) -> Selectable:
        self.connection = connection
        stmt = select([ann_table.c.chrom.label(Vocabulary.CHROM.name),
                       ann_table.c.start.label(Vocabulary.START.name),
                       ann_table.c.stop.label(Vocabulary.STOP.name),
                       ann_table.c.strand.label(Vocabulary.STRAND.name),
                       ann_table.c.gene_type.label(Vocabulary.GENE_TYPE.name)])\
            .where(ann_table.c.gene_name == gene_name)
        if gene_type is not None:
            stmt = stmt.where(ann_table.c.gene_type == gene_type)
        if self.log_sql_statements:
            utils.show_stmt(connection, stmt, logger.debug, 'FIND GENE')
        return stmt

    @staticmethod
    def init_singleton_table():
        global initializing_lock
        global ann_table
        global db_meta
        if ann_table is None:
            # in a racing condition the lock can be acquired as first or as second.
            initializing_lock.acquire(True)
            # if I'm second, the table has been already initialized, so release the lock and exit. If I'm first proceed
            if ann_table is None:
                logger.debug('initializing table for class gencode_v19_hg19')
                db_meta = MetaData()
                connection = None
                try:
                    connection = database.check_and_get_connection()
                    ann_table = Table(table_name,
                                      db_meta,
                                      autoload=True,
                                      autoload_with=connection,
                                      schema=table_schema)
                finally:
                    initializing_lock.release()
                    if connection is not None:
                        connection.close()
            else:
                initializing_lock.release()


