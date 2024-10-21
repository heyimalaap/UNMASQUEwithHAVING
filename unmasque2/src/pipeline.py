from contextlib import ContextDecorator
import time
from loguru import logger

from unmasque2.src.aggregation_extractor import aggregation_extractor
from unmasque2.src.predicate_separator import predicate_separator
from unmasque2.src.projection_extractor import projection_extractor
from .context import UnmasqueContext

from .metadata_extractor import metadata_extractor_stage1, metadata_extractor_stage2
from .from_extractor import from_extractor
from .correlated_sampler import correlated_sampler
from .minimizer import minimizer
from .join_extractor import join_extractor
from .groupby_extractor import groupby_extractor
from .predicate_extractor import predicate_extractor

class Pipeline(ContextDecorator):
    def __init__(self, ctx: UnmasqueContext):
        self.ctx = ctx

    def __enter__(self):
        with logger.contextualize(pipeline='-x-', module='-x-'):
            logger.info('Establishing database connection')
            self.ctx.connection.connect()
        return self

    def __exit__(self, *exc) -> bool:
        with logger.contextualize(pipeline='-x-', module='-x-'):
            logger.info('Restoring database back to its original state')
            self.restore_tables()

            logger.info('Closing database connection')
            self.ctx.connection.close()
        return False

    def backup_tables(self):
        if self.ctx.core_relations is None:
            return 

        for table in self.ctx.core_relations:
            self.ctx.connection.sql(f"ALTER TABLE {table} RENAME TO {table}_restore;", execute_only=True)
            self.ctx.connection.sql(f"CREATE TABLE {table} (LIKE {table}_restore);", execute_only=True)

    def restore_tables(self, sampling_failed=False):
        if self.ctx.core_relations is None:
            return 

        self.ctx.connection.sql("BEGIN TRANSACTION;", execute_only=True)
        
        for table in self.ctx.core_relations:
            self.ctx.connection.sql(f"DROP TABLE {table};", execute_only=True)
            if sampling_failed:
                self.ctx.connection.sql(f"CREATE TABLE {table} (LIKE {table}_restore);", execute_only=True)
                self.ctx.connection.sql(f"INSERT INTO {table} SELECT * FROM {table}_restore;", execute_only=True)
            else:
                self.ctx.connection.sql(f"ALTER TABLE {table}_restore RENAME TO {table};", execute_only=True)

        self.ctx.connection.sql("COMMIT;", execute_only=True)


    def run(self):
        with logger.contextualize(pipeline='Mutation'):
            with logger.contextualize(module='Metadata Extractor I'):
                start_time = time.time()
                metadata_extractor_stage1(self.ctx)
                end_time = time.time()
                self.ctx.metadata_s1_extraction_time = end_time - start_time

            with logger.contextualize(module='From Clause Extractor'):
                start_time = time.time()
                from_extractor(self.ctx)
                end_time = time.time()
                self.ctx.from_extractor_time = end_time - start_time

            with logger.contextualize(module='Metadata Extractor II'):
                start_time = time.time()
                metadata_extractor_stage2(self.ctx)
                end_time = time.time()
                self.ctx.metadata_s2_extraction_time = end_time - start_time

            start_time = time.time()
            self.backup_tables()
            end_time = time.time()
            self.ctx.backup_time = end_time - start_time

            with logger.contextualize(module='Correlated Sampler'):
                start_time = time.time()
                success = correlated_sampler(self.ctx)
                end_time = time.time()
                self.ctx.sampler_time = end_time - start_time
                if not success:
                    logger.warning('Correlated Sampling failed. Using initial database instead.')

            with logger.contextualize(module='Minimizer'):
                start_time = time.time()
                minimizer(self.ctx)
                end_time = time.time()
                self.ctx.minimzer_time = end_time - start_time

            with logger.contextualize(module='Join Graph Extractor'):
                start_time = time.time()
                join_extractor(self.ctx)
                end_time = time.time()
                self.ctx.join_extractor_time = end_time - start_time

            with logger.contextualize(module='Group By Extractor'):
                start_time = time.time()
                groupby_extractor(self.ctx)
                end_time = time.time()
                self.ctx.groupby_extraction_time = end_time - start_time

            with logger.contextualize(module='Predicate Extractor'):
                start_time = time.time()
                predicate_extractor(self.ctx)
                end_time = time.time()
                self.ctx.predicate_extraction_time = end_time - start_time

            with logger.contextualize(module='Projection Extractor'):
                start_time = time.time()
                projection_extractor(self.ctx)
                end_time = time.time()
                self.ctx.projection_extraction_time = end_time - start_time

            with logger.contextualize(module='Aggregation Extractor'):
                start_time = time.time()
                aggregation_extractor(self.ctx)
                end_time = time.time()
                self.ctx.aggregation_extraction_time = end_time - start_time

            with logger.contextualize(module='Predicate Separator'):
                start_time = time.time()
                predicate_separator(self.ctx)
                end_time = time.time()
                self.ctx.predicate_separator_time = end_time - start_time

