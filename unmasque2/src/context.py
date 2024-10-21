from typing import Dict, List, Tuple
from .connection import IConnection
from prettytable import PrettyTable

class UnmasqueContext:
    def __init__(self, connection: IConnection, hidden_query: str):
        self.connection = connection 
        self.hidden_query = hidden_query

        # Done flags
        self.metadata_s1_extraction_done = False
        self.metadata_s2_extraction_done = False
        self.from_extractor_done = False
        self.minimzer_done = False
        self.join_extractor_done = False
        self.groupby_extraction_done = False
        self.predicate_extraction_done = False
        self.projection_extraction_done = False
        self.aggregation_extraction_done = False
        self.predicate_separator_done = False

        # Runtimes
        self.metadata_s1_extraction_time: float = 0
        self.from_extractor_time: float = 0
        self.metadata_s2_extraction_time: float = 0
        self.backup_time: float = 0
        self.sampler_time: float = 0
        self.minimzer_time: float = 0
        self.join_extractor_time: float = 0
        self.groupby_extraction_time: float = 0
        self.predicate_extraction_time: float = 0
        self.projection_extraction_time: float = 0
        self.aggregation_extraction_time: float = 0
        self.predicate_separator_time: float = 0

        # Metadata
        self.db_relations: List[str] | None = None
        self.db_relation_sizes: Dict[str, int] = dict()
        self.pk_dict: Dict[str, str] = dict()
        self.key_lists: List[List[Tuple[str, str]]] = []
        self.db_attribs_types: Dict[str, Dict[str, str]] = dict()
        self.db_attribs_max_length: Dict[str, Dict[str, int]] = dict()

        # From Extractor 
        self.core_relations: List[str] | None = None

        # Minimizer
        self.table_attributes_map: Dict[str, List[str]] = dict()
        self.minimized_attributes: Dict[str, List[str]] = dict()

        # Join Extractor
        self.join_graph: List[List[Tuple[str, str]]] = []

        # Groupby Extractor
        self.groupby_attribs: List[Tuple[str, str]] = []

        # Predicate Extractor
        self.spj_core_size: int = 0
        self.filter_predicates = []
        self.having_predicates = []
        self.separatable_predicates = []

        # Projection Extractor
        self.projected_attrib = []
        self.projection_names = []
        self.projection_deps = []
        self.projection_sol = []

        # Aggregation Extraction
        self.projection_aggregations = []
    
    def set_metadata1(self, db_tables):
        self.metadata_s1_extraction_done = True
        self.db_relations = db_tables

    def set_metadata2(self, db_table_sizes, pk_dict, key_lists, db_attribs_types, db_attribs_max_length):
        self.metadata_s2_extraction_done = True
        self.db_relation_sizes = db_table_sizes
        self.pk_dict = pk_dict
        self.key_lists = key_lists
        self.db_attribs_types = db_attribs_types
        self.db_attribs_max_length = db_attribs_max_length

    def set_from_extractor(self, core_relations):
        self.from_extractor_done = True
        self.core_relations = core_relations

    def set_minimizer(self, table_attrib_map, minimized_attributes):
        self.minimzer_done = True
        self.table_attributes_map = table_attrib_map
        self.minimized_attributes = minimized_attributes

    def set_join_extractor(self, join_graph: List[List[Tuple[str, str]]]):
        self.join_extractor_done = True
        self.join_graph = join_graph

    def set_groupby_extractor(self, groupby_attribs: List[Tuple[str, str]]):
        self.groupby_extraction_done = True
        self.groupby_attribs = groupby_attribs

    def set_predicate_extractor(self, filter_predicates, having_predicates, separatable_predicates):
        self.predicate_extraction_done = True
        self.filter_predicates = filter_predicates
        self.having_predicates = having_predicates
        self.separatable_predicates = separatable_predicates

    def set_projection_extractor(self, projected_attrib, projection_names, projection_deps, projection_sol):
        self.projection_extraction_done = True
        self.projected_attrib = projected_attrib
        self.projection_names = projection_names
        self.projection_deps = projection_deps
        self.projection_sol = projection_sol

    def set_aggregation_extraction(self, projection_aggregations):
        self.aggregation_extraction_done = True
        self.projection_aggregations = projection_aggregations

    def set_predicate_separator(self, filter_predicates, having_predicates):
        self.predicate_separator_done = True
        self.filter_predicates = filter_predicates
        self.having_predicates = having_predicates

    def print_timing(self):
        t = PrettyTable()
        t.field_names = ["Module name", "Time"]
        t.add_rows([
            ["Metadata Extractor I", f'{round(self.metadata_s1_extraction_time, 2)} s'],
            ["From Extractor", f'{round(self.from_extractor_time, 2)} s'],
            ["Metadata Extractor II", f'{round(self.metadata_s2_extraction_time, 2)} s'],
            ["Backup", f'{round(self.backup_time, 2)} s'],
            ["Correlated Sampler", f'{round(self.sampler_time, 2)} s'],
            ["Minimizer", f'{round(self.minimzer_time, 2)} s'],
            ["Join Extractor", f'{round(self.join_extractor_time, 2)} s'],
            ["GroupBy Extractor", f'{round(self.groupby_extraction_time, 2)} s'],
            ["Predicate Extractor", f'{round(self.predicate_extraction_time, 2)} s'],
            ["Projection Extractor", f'{round(self.projection_extraction_time, 2)} s'],
            ["Aggregation Extractor", f'{round(self.aggregation_extraction_time, 2)} s'],
            ["Predicate Separator", f'{round(self.predicate_separator_time, 2)} s'],
        ])

        print(t)
