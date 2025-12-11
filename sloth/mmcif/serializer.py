"""
mmCIF Serializer - Data structure parsers and relationship resolvers

Provides dictionary parsing, mapping generation, caching, and relationship resolution.
"""
import os
import hashlib
import threading
import pickle
import shlex
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple, Set
from abc import ABC, abstractmethod

from .models import MMCIFDataContainer
from .defaults import (
    CacheType, DictDataType, FrameMarker, LoopDataKey, 
    TabularDataCategory, TabularDataField, RelationshipKey, DictItemKey,
    MappingDataKey,
    # Consolidated classes
    DataValue, FileOperation
)


# ====================== Formal Relationship Type Definitions ======================
class RelationshipType:
    """Formal relationship type definitions"""
    COMPOSITIONAL = "compositional"  # Child is part of parent (ownership)
    REFERENTIAL = "referential"      # Child references parent (lookup)
    AGGREGATION = "aggregation"      # Child aggregates parent data
    HIERARCHICAL = "hierarchical"    # Parent-child hierarchy
    UNKNOWN = "unknown"              # Cannot be determined


class RelationshipMetadata:
    """Formal relationship metadata extracted from dictionary"""
    def __init__(
        self,
        child_cat: str,
        child_field: str,
        parent_cat: str,
        parent_field: str,
        relationship_type: str = RelationshipType.UNKNOWN,
        cardinality: Tuple[str, str] = ("1", "*"),
        description: str = "",
        actual_cardinality: Optional[Tuple[str, str]] = None  # Measured from data
    ):
        self.child_cat = child_cat
        self.child_field = child_field
        self.parent_cat = parent_cat
        self.parent_field = parent_field
        self.relationship_type = relationship_type
        self.cardinality = cardinality  # (min, max) for child occurrences per parent
        self.actual_cardinality = actual_cardinality  # Measured from actual data
        self.description = description
    
    def __repr__(self) -> str:
        card_str = f", card={self.actual_cardinality}" if self.actual_cardinality else ""
        return f"RelationshipMetadata({self.child_cat}.{self.child_field} -> {self.parent_cat}.{self.parent_field}, type={self.relationship_type}{card_str})"


class RelationshipConstraint:
    """Represents a formal constraint on relationships"""
    def __init__(self, metadata: RelationshipMetadata, is_validated: bool = False):
        self.metadata = metadata
        self.is_validated = is_validated
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """Validate constraint against actual data"""
        child_data = data.get(self.metadata.child_cat, [])
        parent_data = data.get(self.metadata.parent_cat, [])
        
        if not child_data or not parent_data:
            return False
        
        # Check if child field values are subset of parent field values
        child_values = {row.get(self.metadata.child_field) for row in child_data}
        child_values.discard(None)
        
        parent_values = {row.get(self.metadata.parent_field) for row in parent_data}
        parent_values.discard(None)
        
        return child_values.issubset(parent_values) if child_values and parent_values else False


class CardinalityAnalyzer:
    """Deterministic analysis of relationship cardinality from actual data"""
    
    @staticmethod
    def analyze_cardinality(
        child_data: List[Dict],
        child_field: str,
        parent_data: List[Dict],
        parent_field: str
    ) -> Tuple[str, str]:
        """
        Analyze cardinality: (child_min, child_max) per parent
        
        Returns deterministic cardinality like:
        - ("0", "1"): Zero or one child per parent (optional one-to-one)
        - ("1", "1"): Exactly one child per parent (mandatory one-to-one)
        - ("0", "*"): Zero or more children per parent (optional one-to-many)
        - ("1", "*"): One or more children per parent (mandatory one-to-many)
        """
        if not child_data or not parent_data:
            return ("0", "*")
        
        # Count children per parent
        parent_to_children = {}
        for child_row in child_data:
            fk_value = child_row.get(child_field)
            if fk_value is not None:
                parent_to_children.setdefault(str(fk_value), []).append(child_row)
        
        if not parent_to_children:
            return ("0", "0")  # No relationships exist
        
        # Analyze distribution
        child_counts = [len(children) for children in parent_to_children.values()]
        max_children = max(child_counts)
        min_children = min(child_counts)
        
        # Check if ALL parents have at least one child
        parent_values = {str(row.get(parent_field)) for row in parent_data if row.get(parent_field) is not None}
        all_parents_have_children = parent_values.issubset(set(parent_to_children.keys()))
        
        # Determine cardinality deterministically
        if max_children == 1:
            # One-to-one relationship
            if all_parents_have_children and min_children == 1:
                return ("1", "1")  # Mandatory one-to-one
            else:
                return ("0", "1")  # Optional one-to-one
        else:
            # One-to-many relationship
            if all_parents_have_children and min_children >= 1:
                return ("1", "*")  # Mandatory one-to-many
            else:
                return ("0", "*")  # Optional one-to-many


# ====================== Unified High-Performance Caching ======================
# Global caches for maximum performance (similar to legacy implementation)
_GLOBAL_CACHES = {
    CacheType.DICTIONARY.value: {},
    CacheType.XSD.value: {},
    CacheType.MAPPING_RULES.value: {},
    CacheType.XSD_TREES.value: {}
}
_CACHE_LOCK = threading.Lock()

class CacheManager:
    """
    Unified cache manager that combines global in-memory caching with optional disk persistence.
    Optimized for performance based on legacy implementation patterns.
    """
    
    def __init__(self, cache_dir: Optional[str] = None, enable_disk_cache: bool = True):
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.enable_disk_cache = enable_disk_cache
        if self.cache_dir and enable_disk_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get(self, cache_type: str, key: str) -> Optional[Any]:
        """Get from global cache first, then fallback to disk if enabled"""
        # Fast path: global memory cache
        with _CACHE_LOCK:
            if key in _GLOBAL_CACHES.get(cache_type, {}):
                return _GLOBAL_CACHES[cache_type][key]
        
        # Fallback: disk cache
        if self.enable_disk_cache and self.cache_dir:
            return self._load_from_disk(cache_type, key)
        
        return None
    
    def set(self, cache_type: str, key: str, value: Any) -> None:
        """Store in global cache and optionally on disk"""
        # Always store in global cache for speed
        with _CACHE_LOCK:
            if cache_type not in _GLOBAL_CACHES:
                _GLOBAL_CACHES[cache_type] = {}
            _GLOBAL_CACHES[cache_type][key] = value
        
        # Optionally store on disk for persistence
        if self.enable_disk_cache and self.cache_dir:
            self._save_to_disk(cache_type, key, value)
    
    def _load_from_disk(self, cache_type: str, key: str) -> Optional[Any]:
        """Load from disk cache using pickle for speed"""
        cache_file = self.cache_dir / f"{cache_type}_{key}{FileOperation.PICKLE_EXT.value}"
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, FileOperation.READ_BINARY.value) as f:
                value = pickle.load(f)
                # Also store in global cache for next access
                with _CACHE_LOCK:
                    if cache_type not in _GLOBAL_CACHES:
                        _GLOBAL_CACHES[cache_type] = {}
                    _GLOBAL_CACHES[cache_type][key] = value
                return value
        except Exception:
            # Remove corrupted cache file
            try:
                cache_file.unlink()
            except Exception:
                pass
            return None
    
    def _save_to_disk(self, cache_type: str, key: str, value: Any) -> None:
        """Save to disk cache using pickle for speed"""
        cache_file = self.cache_dir / f"{cache_type}_{key}{FileOperation.PICKLE_EXT.value}"
        try:
            with open(cache_file, FileOperation.WRITE_BINARY.value) as f:
                pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            pass  # Don't fail if we can't cache
    
    @staticmethod
    def clear_global_caches():
        """Clear all global caches"""
        with _CACHE_LOCK:
            for cache_type in _GLOBAL_CACHES:
                _GLOBAL_CACHES[cache_type].clear()

# Create a default cache manager instance
_default_cache_manager = None

def get_cache_manager(cache_dir: Optional[str] = None) -> CacheManager:
    """Get or create the default cache manager"""
    global _default_cache_manager
    if _default_cache_manager is None or (cache_dir and _default_cache_manager.cache_dir != Path(cache_dir)):
        _default_cache_manager = CacheManager(
            cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
        )
    return _default_cache_manager

# ====================== Metadata Parsers ======================
class MetadataParser(ABC):
    """Base class for metadata parsers"""
    def __init__(self, cache_manager: CacheManager, quiet: bool = False):
        self.cache_manager = cache_manager
        self.quiet = quiet

    @abstractmethod
    def parse(self, source: Union[str, Path]) -> Dict[str, Any]:
        pass

class DictionaryParser(MetadataParser):
    """Parses mmCIF dictionary files"""
    def __init__(self, cache_manager: CacheManager, quiet: bool = False):
        super().__init__(cache_manager, quiet)
        self.source = None

    def parse(self, dict_path: Union[str, Path]) -> Dict[str, Any]:
        self.source = dict_path
        if not dict_path or not Path(dict_path).exists():
            return self._empty_dict()
        
        cache_key = self._generate_cache_key(dict_path)
        cached = self.cache_manager.get(CacheType.DICTIONARY.value, cache_key)
        if cached:
            if not self.quiet:
                print("📦 Using cached dictionary data")
            return cached
        
        if not self.quiet:
            print("📚 Parsing dictionary...")
        
        with open(dict_path, FileOperation.READ.value) as f:
            content = f.read()
        
        return self._parse_content(content, dict_path, cache_key)

    def _empty_dict(self) -> Dict[str, Any]:
        """Return empty dictionary structure"""
        return {
            DictDataType.CATEGORIES.value: {},
            DictDataType.ITEMS.value: {},
            DictDataType.RELATIONSHIPS.value: [],
            DictDataType.ENUMERATIONS.value: {},
            DictDataType.ITEM_TYPES.value: {}
        }

    def _generate_cache_key(self, dict_path: Union[str, Path]) -> str:
        """Generate cache key based on file path and modification time"""
        dict_path_resolved = str(Path(dict_path).resolve())
        mtime = os.path.getmtime(dict_path)
        return f"dict_{hashlib.md5(f'{dict_path_resolved}_{mtime}'.encode()).hexdigest()}"

    def _parse_content(self, content: str, dict_path: str, cache_key: str) -> Dict[str, Any]:
        """Parse dictionary content and process frames"""
        import re
        frames = re.split(r'\nsave_', content)
        
        parser = SaveFrameParser(self.quiet)
        processor = FrameDataProcessor(self.quiet)
        
        # Process each save frame
        for frame_content in frames[1:]:
            frame_data = parser.parse_save_frame(frame_content)
            processor.process_frame(frame_data)
        
        # Parse tabular data using MMCIF parser
        tabular_parser = TabularDataParser(self.quiet)
        tabular_parser.parse_tabular_data(dict_path, processor)
        
        # Extract primary keys
        primary_keys = PrimaryKeyExtractor.extract(processor.categories)
        
        result = {
            DictDataType.CATEGORIES.value: processor.categories,
            DictDataType.ITEMS.value: processor.items,
            DictDataType.RELATIONSHIPS.value: processor.relationships,
            DictDataType.ENUMERATIONS.value: processor.enumerations,
            DictDataType.ITEM_TYPES.value: tabular_parser.item_types,
            DictDataType.PRIMARY_KEYS.value: primary_keys
        }
        
        # Debug output
        if not self.quiet:
            print(f"📚 Parsed {len(processor.categories)} categories, {len(processor.items)} items")
            print(f"📝 Found {len(primary_keys)} primary keys:")
            for cat, key in primary_keys.items():
                print(f"  - {cat}: {key}")
        
        # Store in unified cache
        self.cache_manager.set(CacheType.DICTIONARY.value, cache_key, result)
        return result


class SaveFrameParser:
    """Parses individual save frames from dictionary files"""
    def __init__(self, quiet: bool = False):
        self.quiet = quiet
    
    def parse_save_frame(self, frame_content: str) -> Dict[str, Any]:
        """Parse a single save frame into structured data"""
        lines = frame_content.strip().split('\n')
        if not lines:
            return {}
            
        frame_data = {}
        i = 1
        
        while i < len(lines):
            line = lines[i].strip()
            
            if not line or line.startswith(FrameMarker.HASH.value):
                i += 1
                continue
                
            if line == FrameMarker.SAVE_END.value:
                break
                
            if line.startswith(FrameMarker.UNDERSCORE.value) and i + 1 < len(lines) and lines[i + 1].strip() == FrameMarker.MULTILINE_DELIMITER.value:
                frame_data.update(self._parse_multiline(lines, i))
                i = frame_data.pop(LoopDataKey.NEXT_INDEX.value)
                continue
                
            if line.startswith(FrameMarker.UNDERSCORE.value):
                frame_data.update(self._parse_key_value(line))
                i += 1
                continue
                
            if line == FrameMarker.LOOP_START.value:
                loop_data, new_index = self._parse_loop(lines, i + 1)
                frame_data[LoopDataKey.LOOP_DATA.value] = loop_data
                i = new_index
                continue
                
            i += 1
            
        return frame_data

    def _parse_multiline(self, lines: List[str], index: int) -> Dict[str, Any]:
        """Parse multiline text blocks"""
        key = lines[index].strip().strip(FrameMarker.UNDERSCORE.value)
        i = index + 2  # Skip key line and opening ';'
        multiline_content = []
        
        while i < len(lines):
            if lines[i].strip() == FrameMarker.MULTILINE_DELIMITER.value:
                break
            multiline_content.append(lines[i])
            i += 1
        
        return {
            key: '\n'.join(multiline_content).strip(),
            LoopDataKey.NEXT_INDEX.value: i + 1
        }

    def _parse_key_value(self, line: str) -> Dict[str, str]:
        """Parse simple key-value pairs"""
        parts = line.split(None, 1)
        key = parts[0].strip(FrameMarker.UNDERSCORE.value)
        value = parts[1].strip().strip(f'{FileOperation.DOUBLE_QUOTE.value}{FileOperation.SINGLE_QUOTE.value}') if len(parts) == 2 else DataValue.EMPTY_STRING.value
        return {key: value}

    def _parse_loop(self, lines: List[str], start_index: int) -> Tuple[Dict[str, Any], int]:
        """Parse loop structures"""
        i = start_index
        loop_headers = []
        
        # Collect loop headers
        while i < len(lines) and lines[i].strip().startswith(FrameMarker.UNDERSCORE.value):
            loop_headers.append(lines[i].strip().strip(FrameMarker.UNDERSCORE.value))
            i += 1
        
        # Collect loop data
        loop_data = []
        while i < len(lines):
            line = lines[i].strip()
            if not line or line.startswith(FrameMarker.HASH.value) or line in (FrameMarker.SAVE_END.value, FrameMarker.LOOP_START.value) or line.startswith(FrameMarker.UNDERSCORE.value):
                break
                
            try:
                row_data = shlex.split(line)
            except ValueError:
                row_data = line.split()
                
            if len(row_data) >= len(loop_headers):
                loop_data.append(row_data)
            i += 1
        
        # Format loop data
        loop_items = []
        for row in loop_data:
            row_data = {}
            for j, header in enumerate(loop_headers):
                if j < len(row):
                    row_data[header] = row[j].strip(f'{FileOperation.DOUBLE_QUOTE.value}{FileOperation.SINGLE_QUOTE.value}')
            loop_items.append(row_data)
        
        return {
            LoopDataKey.HEADERS.value: loop_headers,
            LoopDataKey.ITEMS.value: loop_items
        }, i


class FrameDataProcessor:
    """Processes parsed frame data into dictionary structures"""
    def __init__(self, quiet: bool = False):
        self.quiet = quiet
        self.categories = {}
        self.items = {}
        self.relationships = []
        self.enumerations = {}
    
    def process_frame(self, frame_data: Dict[str, Any]):
        """Process a single frame's data"""
        if LoopDataKey.LOOP_DATA.value in frame_data:
            self._process_loop_frame(frame_data)
        else:
            self._process_non_loop_frame(frame_data)
    
    def _process_loop_frame(self, frame_data: Dict[str, Any]):
        """Process frames with loop data"""
        loop_info = frame_data[LoopDataKey.LOOP_DATA.value]
        
        for loop_item in loop_info[LoopDataKey.ITEMS.value]:
            combined_data = {**frame_data, **loop_item}
            self._classify_data(combined_data)
    
    def _process_non_loop_frame(self, frame_data: Dict[str, Any]):
        """Process frames without loop data"""
        self._classify_data(frame_data)
    
    def _classify_data(self, data: Dict[str, Any]):
        """Classify data into categories, items, or relationships"""
        if DictItemKey.CATEGORY_ID.value in data:
            self.categories[data[DictItemKey.CATEGORY_ID.value]] = data
        elif DictItemKey.ITEM_NAME.value in data:
            item_name = data[DictItemKey.ITEM_NAME.value].strip(f'{FileOperation.DOUBLE_QUOTE.value}{FileOperation.SINGLE_QUOTE.value}')
            self.items[item_name] = data
            self._process_enumeration(data, item_name)
        elif RelationshipKey.ITEM_LINKED_CHILD_NAME.value in data and RelationshipKey.ITEM_LINKED_PARENT_NAME.value in data:
            self.relationships.append(data)
        elif RelationshipKey.PDBX_CHILD_CATEGORY_ID.value in data:
            self._process_group_list(data)

    def _process_enumeration(self, data: Dict[str, Any], item_name: str):
        """Process enumeration values if present"""
        if DictItemKey.ITEM_ENUMERATION_VALUE.value in data:
            values = data[DictItemKey.ITEM_ENUMERATION_VALUE.value]
            if isinstance(values, str):
                values = [values]
            self.enumerations[item_name] = values

    def _process_group_list(self, data: Dict[str, Any]):
        """Process pdbx_item_linked_group_list entries"""
        child_cat = data.get(RelationshipKey.PDBX_CHILD_CATEGORY_ID.value)
        child_name = data.get(RelationshipKey.PDBX_CHILD_NAME.value)
        parent_name = data.get(RelationshipKey.PDBX_PARENT_NAME.value)
        parent_cat = data.get(RelationshipKey.PDBX_PARENT_CATEGORY_ID.value)
        
        if child_cat and child_name and parent_name and parent_cat:
            self.relationships.append({
                RelationshipKey.CHILD_CATEGORY.value: child_cat,
                RelationshipKey.CHILD_NAME.value: child_name,
                RelationshipKey.PARENT_CATEGORY.value: parent_cat,
                RelationshipKey.PARENT_NAME.value: parent_name
            })


class TabularDataParser:
    """Parses tabular data from dictionary files"""
    def __init__(self, quiet: bool = False):
        self.quiet = quiet
        self.item_types = {}
    
    def parse_tabular_data(self, dict_path: str, processor: FrameDataProcessor):
        """Parse tabular data using MMCIFParser"""
        try:
            from .parser import MMCIFParser
            parser = MMCIFParser()
            container = parser.parse(dict_path)
            
            self._process_item_types(container)
            self._process_linked_groups(container, processor)
            
        except Exception as e:
            if not self.quiet:
                print(f"Warning: Could not parse tabular data: {e}")
                import traceback
                traceback.print_exc()
    
    def _process_item_types(self, container):
        """Extract item type information"""
        if TabularDataCategory.ITEM_TYPE_LIST.value in container[0].data:
            type_list = container[0].data[TabularDataCategory.ITEM_TYPE_LIST.value]
            for i in range(type_list.row_count):
                row = type_list[i].data
                code = row.get(TabularDataField.CODE.value)
                if code:
                    self.item_types[code] = row
    
    def _process_linked_groups(self, container, processor):
        """Extract relationships from pdbx_item_linked_group_list"""
        if TabularDataCategory.PDBX_ITEM_LINKED_GROUP_LIST.value in container[0].data:
            linked_list = container[0].data[TabularDataCategory.PDBX_ITEM_LINKED_GROUP_LIST.value]
            if not self.quiet:
                print(f"📊 Found {linked_list.row_count} relationships in dictionary")
            for i in range(linked_list.row_count):
                row = linked_list[i].data
                child_cat = row.get(TabularDataField.CHILD_CATEGORY_ID.value)
                child_name = row.get(TabularDataField.CHILD_NAME.value, DataValue.EMPTY_STRING.value).strip(FileOperation.DOUBLE_QUOTE.value)
                parent_name = row.get(TabularDataField.PARENT_NAME.value, DataValue.EMPTY_STRING.value).strip(FileOperation.DOUBLE_QUOTE.value)
                parent_cat = row.get(TabularDataField.PARENT_CATEGORY_ID.value)
                
                if child_cat and child_name and parent_name and parent_cat:
                    processor.relationships.append({
                        RelationshipKey.CHILD_CATEGORY.value: child_cat,
                        RelationshipKey.CHILD_NAME.value: child_name,
                        RelationshipKey.PARENT_CATEGORY.value: parent_cat,
                        RelationshipKey.PARENT_NAME.value: parent_name
                    })


class PrimaryKeyExtractor:
    """Extracts primary key information from categories"""
    @staticmethod
    def extract(categories: Dict[str, Any]) -> Dict[str, Union[str, List[str]]]:
        """Extract primary keys from category data"""
        primary_keys = {}
        for cat_name, cat_data in categories.items():
            key_items = []
            
            # Check for direct key field
            if DictItemKey.CATEGORY_KEY_NAME.value in cat_data:
                key_item = cat_data[DictItemKey.CATEGORY_KEY_NAME.value].strip(f'{FileOperation.DOUBLE_QUOTE.value}{FileOperation.SINGLE_QUOTE.value}')
                if key_item:
                    key_items.append(key_item)
            
            # Check for composite keys in loop data
            if LoopDataKey.LOOP_DATA.value in cat_data:
                loop_data = cat_data[LoopDataKey.LOOP_DATA.value]
                for item in loop_data[LoopDataKey.ITEMS.value]:
                    if DictItemKey.CATEGORY_KEY_NAME.value in item:
                        key_item = item[DictItemKey.CATEGORY_KEY_NAME.value].strip(f'{FileOperation.DOUBLE_QUOTE.value}{FileOperation.SINGLE_QUOTE.value}')
                        if key_item and key_item not in key_items:
                            key_items.append(key_item)
            
            # Process found key items
            if key_items:
                fields = []
                for key_item in key_items:
                    if key_item.startswith(FrameMarker.UNDERSCORE.value) and DataValue.DOT.value in key_item:
                        field_name = key_item.split(DataValue.DOT.value)[-1]
                        fields.append(field_name)
                if fields:
                    primary_keys[cat_name] = fields[0] if len(fields) == 1 else fields
        
        return primary_keys

# ====================== Mapping Generator ======================
class MappingGenerator:
    """Generates mapping rules from mmCIF dictionary metadata"""
    def __init__(
        self, 
        dict_parser: DictionaryParser,
        cache_manager: CacheManager,
        quiet: bool = False
    ):
        self.dict_parser = dict_parser
        self.cache_manager = cache_manager
        self.quiet = quiet
        self._mapping_rules = None

    def get_mapping_rules(self) -> Dict[str, Any]:
        if self._mapping_rules is not None:
            return self._mapping_rules
        
        cache_key = self._generate_cache_key()
        cached = self.cache_manager.get(CacheType.MAPPING_RULES.value, cache_key)
        if cached:
            self._mapping_rules = cached
            if not self.quiet:
                print("📦 Using cached mapping rules")
            return cached
        
        if not self.quiet:
            print("🧩 Generating mapping rules...")
            
        dict_meta = self.dict_parser.parse(self.dict_parser.source)
        
        self._mapping_rules = self._generate_mapping(dict_meta)
        self.cache_manager.set(CacheType.MAPPING_RULES.value, cache_key, self._mapping_rules)
        return self._mapping_rules

    def _generate_cache_key(self) -> str:
        """Generate cache key based on source files and modification times"""
        cache_key_parts = []
        if self.dict_parser.source and Path(self.dict_parser.source).exists():
            dict_path = str(Path(self.dict_parser.source).resolve())
            dict_mtime = os.path.getmtime(self.dict_parser.source)
            cache_key_parts.append(f"dict_{dict_path}_{dict_mtime}")
        
        return f"mapping_{hashlib.md5('|'.join(cache_key_parts).encode()).hexdigest()}"

    def _generate_mapping(
        self, 
        dict_meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate complete mapping rules from dictionary metadata"""
        builder = MappingBuilder(dict_meta)
        builder.build_primary_mappings()
        builder.build_foreign_key_map()
        
        return {
            MappingDataKey.CATEGORY_MAPPING.value: builder.category_mapping,
            MappingDataKey.ITEM_MAPPING.value: builder.item_mapping,
            MappingDataKey.FK_MAP.value: builder.fk_map,
            DictDataType.PRIMARY_KEYS.value: dict_meta.get(DictDataType.PRIMARY_KEYS.value, {})
        }


class MappingBuilder:
    """Builds mapping rules from mmCIF dictionary metadata"""
    def __init__(self, dict_meta: Dict[str, Any]):
        self.dict_meta = dict_meta
        self.category_mapping = {}
        self.item_mapping = {}
        self.fk_map = {}
        self._temp_fk_relationships = {}  # Stores all FK candidates before priority selection
    
    def build_primary_mappings(self):
        """Build primary category and item mappings"""
        for cat_name, cat_data in self.dict_meta[DictDataType.CATEGORIES.value].items():
            self._process_category(cat_name, cat_data)
    
    def _process_category(self, cat_name: str, _cat_data: Dict[str, Any]):
        """Process a single category from dictionary metadata"""
        # Get all items for this category
        cat_items = self._get_category_items(cat_name)
        
        # Create category mapping
        self.category_mapping[cat_name] = {MappingDataKey.FIELDS.value: sorted(list(cat_items))}
        
        # Map individual items
        self.item_mapping[cat_name] = {}
        for field_name in cat_items:
            self._map_item(cat_name, field_name)
    
    def _get_category_items(self, cat_name: str) -> Set[str]:
        """Get all item names for a category"""
        cat_items = set()
        for item_name in self.dict_meta[DictDataType.ITEMS.value]:
            if item_name.startswith(f"{FrameMarker.UNDERSCORE.value}{cat_name}{DataValue.DOT.value}"):
                field_name = item_name[len(f"{FrameMarker.UNDERSCORE.value}{cat_name}{DataValue.DOT.value}"):]
                cat_items.add(field_name)
        return cat_items
    
    def _map_item(self, cat_name: str, field_name: str):
        """Map a single item from dictionary metadata"""
        item_name = f"_{cat_name}.{field_name}"
        item_data = self.dict_meta['items'].get(item_name, {})
        
        # Create item mapping
        self.item_mapping[cat_name][field_name] = {
            "type": item_data.get("item_type.code", "string"),
            "enum": self.dict_meta['enumerations'].get(item_name),
            "description": item_data.get("item_description.description", "")
        }
    
    def build_foreign_key_map(self):
        """Build foreign key mapping from relationships"""
        # First pass: collect all relationships
        for rel in self.dict_meta['relationships']:
            self._process_relationship(rel)
        
        # Second pass: resolve collisions using priority-based selection
        self._resolve_fk_collisions()
    
    def _process_relationship(self, rel: Dict[str, Any]):
        """Process a single relationship entry and store for collision resolution"""
        # Extract relationship data
        child_name = rel.get("item_linked.child_name") or rel.get("child_name")
        parent_name = rel.get("item_linked.parent_name") or rel.get("parent_name")
        child_cat = rel.get("child_category")
        parent_cat = rel.get("parent_category")
        
        # Skip if missing required data
        if not child_name or not parent_name:
            return
        
        # Handle different relationship formats
        if child_cat and parent_cat:
            # New format with explicit categories
            child_field = self._extract_field_name(child_name)
            parent_field = self._extract_field_name(parent_name)
            child_key = (child_cat, child_field)
            parent_value = (parent_cat, parent_field)
            
            # Store relationship (may be multiple per child field)
            if child_key not in self._temp_fk_relationships:
                self._temp_fk_relationships[child_key] = []
            self._temp_fk_relationships[child_key].append(parent_value)
        else:
            # Legacy format - extract from field names
            self._process_legacy_relationship(child_name, parent_name)
    
    def _extract_field_name(self, name: str) -> str:
        """Extract field name from full item name"""
        return name.strip("_").split(".")[-1] if "." in name else name
    
    def _process_legacy_relationship(self, child_name: str, parent_name: str):
        """Process legacy relationship format"""
        child_parts = child_name.strip("_").split(".")
        parent_parts = parent_name.strip("_").split(".")
        
        if len(child_parts) == 2 and len(parent_parts) == 2:
            child_key = (child_parts[0], child_parts[1])
            parent_value = (parent_parts[0], parent_parts[1])
            
            # Store relationship (may be multiple per child field)
            if child_key not in self._temp_fk_relationships:
                self._temp_fk_relationships[child_key] = []
            self._temp_fk_relationships[child_key].append(parent_value)
    
    def _resolve_fk_collisions(self):
        """Resolve FK collisions using priority-based selection.
        
        When multiple foreign key targets exist for the same child field,
        select the "most primary" parent using these heuristics:
        1. Prefer parents with simpler names (fewer underscores)
        2. Prefer non-prefixed names over pdbx_/cif_ prefixed names
        3. Prefer shorter category names (more general)
        
        This ensures that subtype patterns like entity_poly -> entity
        and pdbx_entity_nonpoly -> entity are correctly identified.
        """
        for child_key, parent_candidates in self._temp_fk_relationships.items():
            if len(parent_candidates) == 1:
                # No collision, use the single relationship
                self.fk_map[child_key] = parent_candidates[0]
            else:
                # Collision detected - select best parent by priority
                best_parent = self._select_primary_parent(parent_candidates)
                self.fk_map[child_key] = best_parent
    
    def _select_primary_parent(self, candidates: List[Tuple[str, str]]) -> Tuple[str, str]:
        """Select the most primary parent from multiple candidates.
        
        Priority rules (lower score = more primary):
        1. Count underscores in category name (fewer is better)
        2. Penalize pdbx_/cif_ prefixes (indicates extension tables)
        3. Prefer shorter names (more general concepts)
        """
        def parent_priority_score(parent: Tuple[str, str]) -> Tuple[int, int, int]:
            parent_cat, _parent_field = parent
            
            # Count underscores (fewer = more primary)
            underscore_count = parent_cat.count('_')
            
            # Penalize extension prefixes
            has_prefix = 1 if parent_cat.startswith(('pdbx_', 'cif_', 'rcsb_')) else 0
            
            # Prefer shorter names (more general)
            name_length = len(parent_cat)
            
            return (underscore_count, has_prefix, name_length)
        
        # Sort by priority score (lowest = best)
        return min(candidates, key=parent_priority_score)


# ====================== Relationship Resolver ======================
class RelationshipResolver:
    """Resolves entity relationships for nested JSON output from mmCIF data"""
    def __init__(
        self, 
        mapping_generator: MappingGenerator,
        enable_semantic_fallbacks: bool = False,
        domain_overrides: Optional[Dict[Tuple[str, str, str, str], bool]] = None,
        strict_structural_only: bool = False
    ):
        self.mapping_generator = mapping_generator
        self.ownership_analyzer = OwnershipAnalyzer(
            self.mapping_generator, 
            enable_semantic_fallbacks=enable_semantic_fallbacks,
            domain_overrides=domain_overrides,
            strict_structural_only=strict_structural_only
        )
        self.nesting_builder = NestingBuilder()
        
    @property
    def mapping_rules(self) -> Dict[str, Any]:
        """Cached access to mapping rules"""
        return self.mapping_generator.get_mapping_rules()

    def resolve_relationships(self, mmcif_data: MMCIFDataContainer) -> Dict[str, Any]:
        """Resolve relationships directly from mmCIF data to create nested JSON"""
        # Convert mmCIF container to flat dict
        flat = self._flatten_mmcif(mmcif_data)
        
        # Get mapping rules
        mapping = self.mapping_rules
        fk_map = mapping["fk_map"]
        primary_keys = mapping.get("primary_keys", {})
        
        # Filter FK map to only include ownership relationships
        ownership_fk_map = self.ownership_analyzer.filter_ownership_relationships(fk_map, flat)
        
        # Build nested structure
        return self.nesting_builder.build_nested_structure(flat, ownership_fk_map, primary_keys)
    
    def get_decision_report(self) -> Dict[str, Any]:
        """Get report on how ownership relationships were determined"""
        return self.ownership_analyzer.get_decision_report()
    
    def get_cardinality_report(self) -> Dict[str, Dict[str, Any]]:
        """Get report on measured cardinalities for all relationships"""
        report = {}
        for constraint in self.ownership_analyzer.constraints:
            if constraint.metadata.actual_cardinality:
                key = f"{constraint.metadata.child_cat}.{constraint.metadata.child_field} -> {constraint.metadata.parent_cat}.{constraint.metadata.parent_field}"
                report[key] = {
                    "cardinality": constraint.metadata.actual_cardinality,
                    "relationship_type": constraint.metadata.relationship_type,
                    "validated": constraint.is_validated
                }
        return report
    
    def _flatten_mmcif(self, mmcif_data: MMCIFDataContainer) -> Dict[str, Any]:
        """Convert mmCIF container to flat dictionary structure"""
        flat = {}
        
        for block in mmcif_data:
            for category_name, category in block.data.items():
                # Remove underscore prefix from category name
                entity_name = category_name.lstrip('_')
                
                # Convert each row to a dictionary
                for row in category:
                    row_data = row.data
                    flat.setdefault(entity_name, []).append(row_data)
        
        return flat


class ConstraintExtractor:
    """Extracts formal relationship constraints from dictionary metadata"""
    def __init__(self, dict_meta: Dict[str, Any], data: Optional[Dict[str, Any]] = None):
        self.dict_meta = dict_meta
        self.data = data  # Actual data for cardinality analysis
    
    def extract_constraints(self) -> List[RelationshipConstraint]:
        """Extract formal relationship constraints from dictionary"""
        constraints = []
        
        for rel in self.dict_meta.get('relationships', []):
            metadata = self._build_relationship_metadata(rel)
            if metadata:
                constraints.append(RelationshipConstraint(metadata))
        
        return constraints
    
    def _build_relationship_metadata(self, rel: Dict[str, Any]) -> Optional[RelationshipMetadata]:
        """Build relationship metadata from dictionary entry"""
        # Extract relationship data
        child_name = rel.get("item_linked.child_name") or rel.get("child_name")
        parent_name = rel.get("item_linked.parent_name") or rel.get("parent_name")
        child_cat = rel.get("child_category")
        parent_cat = rel.get("parent_category")
        
        if not child_name or not parent_name:
            return None
        
        # Extract field names
        if child_cat and parent_cat:
            child_field = self._extract_field_name(child_name)
            parent_field = self._extract_field_name(parent_name)
        else:
            # Legacy format
            child_parts = child_name.strip("_").split(".")
            parent_parts = parent_name.strip("_").split(".")
            
            if len(child_parts) != 2 or len(parent_parts) != 2:
                return None
            
            child_cat, child_field = child_parts
            parent_cat, parent_field = parent_parts
        
        # Determine relationship type from dictionary metadata
        rel_type = self._determine_relationship_type(rel, child_cat, child_field, parent_cat)
        
        # Analyze actual cardinality if data is available
        actual_cardinality = None
        if self.data:
            child_data = self.data.get(child_cat, [])
            parent_data = self.data.get(parent_cat, [])
            if child_data and parent_data:
                actual_cardinality = CardinalityAnalyzer.analyze_cardinality(
                    child_data, child_field, parent_data, parent_field
                )
        
        return RelationshipMetadata(
            child_cat=child_cat,
            child_field=child_field,
            parent_cat=parent_cat,
            parent_field=parent_field,
            relationship_type=rel_type,
            description=rel.get('description', ''),
            actual_cardinality=actual_cardinality
        )
    
    def _extract_field_name(self, name: str) -> str:
        """Extract field name from full item name"""
        return name.strip("_").split(".")[-1] if "." in name else name
    
    def _determine_relationship_type(self, rel: Dict, child_cat: str, 
                                    child_field: str, parent_cat: str) -> str:
        """Determine relationship type using explicit dictionary metadata and deterministic rules"""
        description = rel.get('description', '').lower()
        
        # Rule 1: Check explicit ownership indicators in dictionary
        ownership_terms = ['belongs to', 'owned by', 'part of', 'contained in', 'member of']
        if any(term in description for term in ownership_terms):
            return RelationshipType.COMPOSITIONAL
        
        # Rule 2: Check explicit reference indicators in dictionary
        reference_terms = ['refers to', 'references', 'lookup', 'type of', 'code for', 'category of']
        if any(term in description for term in reference_terms):
            return RelationshipType.REFERENTIAL
        
        # Rule 3: Deterministic naming pattern analysis
        # Pattern: field named <parent_cat>_id indicates compositional
        if child_field == f'{parent_cat}_id' or child_field == parent_cat:
            return RelationshipType.COMPOSITIONAL
        
        # Pattern: field ending with _type, _code, _symbol indicates referential
        if any(child_field.endswith(suffix) for suffix in ['_type', '_code', '_symbol', '_method', '_class']):
            return RelationshipType.REFERENTIAL
        
        # Rule 4: Category hierarchy - child contains parent name
        if parent_cat in child_cat and parent_cat != child_cat:
            return RelationshipType.COMPOSITIONAL
        
        # Rule 5: Lookup table patterns (deterministic category name patterns)
        lookup_patterns = ['_type', '_class', '_method', '_status', '_code', '_symbol', 
                          '_enum', '_dict', '_list', '_table', '_ref']
        if any(pattern in parent_cat for pattern in lookup_patterns):
            return RelationshipType.REFERENTIAL
        
        return RelationshipType.UNKNOWN


class RelationshipGraph:
    """Formal graph representation of relationships"""
    def __init__(self):
        self.nodes: Set[str] = set()
        self.edges: Dict[str, List[Tuple[str, str]]] = {}  # parent -> [(child, edge_type)]
        self.reverse_edges: Dict[str, List[Tuple[str, str]]] = {}  # child -> [(parent, edge_type)]
    
    def add_edge(self, parent: str, child: str, edge_type: str):
        """Add directed edge from parent to child"""
        self.nodes.add(parent)
        self.nodes.add(child)
        
        if parent not in self.edges:
            self.edges[parent] = []
        self.edges[parent].append((child, edge_type))
        
        if child not in self.reverse_edges:
            self.reverse_edges[child] = []
        self.reverse_edges[child].append((parent, edge_type))
    
    def get_children(self, node: str, edge_type: Optional[str] = None) -> List[str]:
        """Get children of a node, optionally filtered by edge type"""
        children = self.edges.get(node, [])
        if edge_type:
            return [child for child, et in children if et == edge_type]
        return [child for child, _ in children]
    
    def get_parents(self, node: str, edge_type: Optional[str] = None) -> List[str]:
        """Get parents of a node, optionally filtered by edge type"""
        parents = self.reverse_edges.get(node, [])
        if edge_type:
            return [parent for parent, et in parents if et == edge_type]
        return [parent for parent, _ in parents]
    
    def find_roots(self, edge_type: Optional[str] = None) -> Set[str]:
        """Find root nodes (nodes with no parents) for given edge type"""
        all_children = set()
        all_parents = set()
        
        for parent, children in self.edges.items():
            for child, et in children:
                if edge_type is None or et == edge_type:
                    all_children.add(child)
                    all_parents.add(parent)
        
        return all_parents - all_children
    
    def has_cycle(self) -> bool:
        """Check if graph has cycles using DFS"""
        visited = set()
        rec_stack = set()
        
        def visit(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for child, _ in self.edges.get(node, []):
                if child not in visited:
                    if visit(child):
                        return True
                elif child in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        for node in self.nodes:
            if node not in visited:
                if visit(node):
                    return True
        
        return False


class OwnershipAnalyzer:
    """
    Analyzes relationships to determine ownership using structural algorithms.
    
    Rule Hierarchy (applied in order):
    0. Domain overrides (explicit expert knowledge)
    1. PK extension (CORE STRUCTURAL - child PK extends parent PK)
    2. Single-FK child (CORE STRUCTURAL - detail table with one FK)
    3. Strong FK dependency (STRUCTURAL/SEMANTIC - mandatory FK + name tokens)
    4. Explicit constraint type (dictionary metadata)
    5. Semantic fallbacks (HEURISTIC - optional, disabled in strict mode)
    
    Modes:
    - strict_structural_only=True: Only rules 0, 1, 2 (pure PK/FK structure)
    - strict_structural_only=False: Rules 0-4 (includes strong FK dependency)
    - enable_semantic_fallbacks=True: All rules including heuristics
    """
    def __init__(
        self, 
        mapping_generator: MappingGenerator, 
        enable_semantic_fallbacks: bool = False,
        domain_overrides: Optional[Dict[Tuple[str, str, str, str], bool]] = None,
        strict_structural_only: bool = False
    ):
        self.mapping_generator = mapping_generator
        self.enable_semantic_fallbacks = enable_semantic_fallbacks
        self.strict_structural_only = strict_structural_only
        self.domain_overrides = domain_overrides or {}  # (child_cat, child_field, parent_cat, parent_field) -> is_ownership
        self.graph: Optional[RelationshipGraph] = None
        self.constraints: List[RelationshipConstraint] = []
        self.decision_log: List[Dict[str, Any]] = []  # Audit trail of decisions

    def filter_ownership_relationships(self, fk_map: Dict, data: Dict) -> Dict:
        """Filter FK map to include only ownership relationships using structural analysis"""
        # Get dictionary metadata for structural analysis
        dict_meta = self.mapping_generator.dict_parser.parse(
            self.mapping_generator.dict_parser.source
        )
        primary_keys = dict_meta.get(DictDataType.PRIMARY_KEYS.value, {})
        
        # Extract formal constraints from dictionary with actual data for cardinality analysis
        extractor = ConstraintExtractor(dict_meta, data)
        self.constraints = extractor.extract_constraints()
        
        # Validate constraints against actual data
        for constraint in self.constraints:
            constraint.is_validated = constraint.validate(data)
        
        # Build relationship graph
        self.graph = self._build_relationship_graph()
        
        # Filter FK map based on structural ownership analysis
        ownership_fk_map = {}
        
        for (child_cat, child_field), (parent_cat, parent_field) in fk_map.items():
            if self._is_ownership_relationship(
                child_cat, child_field, parent_cat, parent_field, fk_map, primary_keys, dict_meta
            ):
                ownership_fk_map[(child_cat, child_field)] = (parent_cat, parent_field)
        
        return ownership_fk_map
    
    def _build_relationship_graph(self) -> RelationshipGraph:
        """Build formal relationship graph from validated constraints"""
        graph = RelationshipGraph()
        
        for constraint in self.constraints:
            if constraint.is_validated:
                meta = constraint.metadata
                graph.add_edge(
                    parent=meta.parent_cat,
                    child=meta.child_cat,
                    edge_type=meta.relationship_type
                )
        
        return graph
    
    def _is_ownership_relationship(
        self, 
        child_cat: str, 
        child_field: str, 
        parent_cat: str, 
        parent_field: str,
        fk_map: Dict,
        primary_keys: Dict[str, Union[str, List[str]]],
        dict_meta: Dict
    ) -> bool:
        """
        Determine ownership using structural analysis of the schema.
        
        Ownership is defined structurally:
        1. Domain overrides (expert knowledge)
        2. Child's PK extends parent's PK (key structure hierarchy)
        3. Child has single FK and no outgoing references (detail table)
        4. FK field is mandatory and references parent's PK (strong dependency)
        5. Explicit relationship type from validated constraints
        6. (Optional) Semantic patterns from dictionary metadata
        
        All rules are generic and derived from the dictionary schema.
        """
        decision_key = (child_cat, child_field, parent_cat, parent_field)
        
        # Rule 0: Domain overrides take precedence
        if decision_key in self.domain_overrides:
            decision = self.domain_overrides[decision_key]
            self._log_decision(child_cat, child_field, parent_cat, parent_field, 
                             decision, "domain_override")
            return decision
        
        # Rule 1: Structural ownership via key extension
        if self._is_ownership_structural(child_cat, child_field, parent_cat, parent_field, primary_keys):
            self._log_decision(child_cat, child_field, parent_cat, parent_field, 
                             True, "pk_extension")
            return True
        
        # Rule 2: Single-FK child ownership (detail/dependent table)
        # CORE STRUCTURAL RULE
        if self._is_single_fk_child_ownership(child_cat, child_field, parent_cat, fk_map, primary_keys):
            self._log_decision(child_cat, child_field, parent_cat, parent_field, 
                             True, "single_fk_child", confidence="high")
            return True
        
        # In strict structural mode, stop here (only PK/FK structure rules)
        if self.strict_structural_only:
            self._log_decision(child_cat, child_field, parent_cat, parent_field, 
                             False, "strict_structural_default", confidence="high")
            return False
        
        # Rule 3: Strong FK dependency (mandatory FK to parent's PK)
        # STRUCTURAL/SEMANTIC RULE (uses mandatory flag + name tokens)
        if self._is_strong_fk_dependency(child_cat, child_field, parent_cat, parent_field, primary_keys, dict_meta):
            self._log_decision(child_cat, child_field, parent_cat, parent_field, 
                             True, "strong_fk_dependency", confidence="medium")
            return True
        
        # Rule 4: Check explicit relationship type from validated constraints
        for constraint in self.constraints:
            meta = constraint.metadata
            if (meta.child_cat == child_cat and meta.child_field == child_field and
                meta.parent_cat == parent_cat and meta.parent_field == parent_field):
                if constraint.is_validated:
                    decision = meta.relationship_type == RelationshipType.COMPOSITIONAL
                    self._log_decision(child_cat, child_field, parent_cat, parent_field, 
                                     decision, f"explicit_constraint_{meta.relationship_type}")
                    return decision
        
        # Optional semantic fallbacks (explicitly marked as heuristic)
        if self.enable_semantic_fallbacks:
            decision = self._apply_semantic_fallbacks(child_cat, child_field, parent_cat, dict_meta)
            self._log_decision(child_cat, child_field, parent_cat, parent_field, 
                             decision, "semantic_fallback")
            return decision
        
        # Default: not ownership
        self._log_decision(child_cat, child_field, parent_cat, parent_field, 
                         False, "default_referential")
        return False
    
    def _log_decision(self, child_cat: str, child_field: str, parent_cat: str, 
                     parent_field: str, decision: bool, rule: str, confidence: str = "medium"):
        """Log ownership decision for audit trail with confidence level"""
        self.decision_log.append({
            "relationship": f"{child_cat}.{child_field} -> {parent_cat}.{parent_field}",
            "is_ownership": decision,
            "rule_applied": rule,
            "confidence": confidence  # "high" (structural), "medium" (structural/semantic), "low" (heuristic)
        })
    
    def get_decision_report(self) -> Dict[str, Any]:
        """Generate report on how relationships were determined"""
        report = {
            "total_decisions": len(self.decision_log),
            "ownership_count": sum(1 for d in self.decision_log if d["is_ownership"]),
            "referential_count": sum(1 for d in self.decision_log if not d["is_ownership"]),
            "rules_applied": {},
            "confidence_levels": {
                "high": 0,
                "medium": 0,
                "low": 0
            },
            "mode": "strict_structural" if self.strict_structural_only else ("heuristic" if self.enable_semantic_fallbacks else "standard")
        }
        
        for decision in self.decision_log:
            rule = decision["rule_applied"]
            report["rules_applied"].setdefault(rule, 0)
            report["rules_applied"][rule] += 1
            
            confidence = decision.get("confidence", "medium")
            report["confidence_levels"][confidence] += 1
        
        return report
    
    def _is_ownership_structural(
        self,
        child_cat: str,
        child_field: str,
        parent_cat: str,
        parent_field: str,
        primary_keys: Dict[str, Union[str, List[str]]]
    ) -> bool:
        """
        CORE STRUCTURAL RULE: Child's PK extends parent's PK.
        Confidence: HIGH (pure PK/FK structure analysis)
        
        Example: parent PK = {id}, child PK = {id, ordinal}
        This indicates child is a detail/component of parent.
        
        Algorithm:
        1. Normalize PKs to sets
        2. Verify FK references parent's PK
        3. Verify FK is part of child's PK
        4. Verify parent PK ⊆ child PK and parent PK ≠ child PK
        
        Pure structural analysis - no category names, no hardcoding.
        Deterministic for a given dictionary.
        """
        # Normalize PKs to sets
        def norm_pk(pk):
            if pk is None:
                return set()
            if isinstance(pk, str):
                return {pk}
            return set(pk)
        
        pk_child = norm_pk(primary_keys.get(child_cat))
        pk_parent = norm_pk(primary_keys.get(parent_cat))
        
        # No primary key info → can't determine structurally
        if not pk_child or not pk_parent:
            return False
        
        # The FK must hit the parent's PK
        if parent_field not in pk_parent:
            return False
        
        # The child FK must be part of the child's PK
        if child_field not in pk_child:
            return False
        
        # Ownership: child's PK extends parent's PK
        # Parent's PK is subset of child's PK, and they're not identical
        return pk_parent.issubset(pk_child) and pk_child != pk_parent
    
    def _is_single_fk_child_ownership(
        self,
        child_cat: str,
        child_field: str,
        parent_cat: str,
        fk_map: Dict[Tuple[str, str], Tuple[str, str]],
        primary_keys: Dict[str, Union[str, List[str]]]
    ) -> bool:
        """
        CORE STRUCTURAL RULE: Category with exactly one FK is owned by that parent.
        Confidence: HIGH (pure FK/PK graph structure)
        
        Algorithm:
        1. Count FKs where child_cat is the child (must be exactly 1)
        2. Verify it's the relationship being examined
        3. Check if child has no PK → detail table → ownership
        4. If child has PK, check if it's a surrogate key:
           - Single-field PK only
           - Not referenced as parent elsewhere
        
        This catches detail/dependent tables that:
        - Have only one foreign key relationship
        - Have no PK, or have a surrogate PK not referenced elsewhere
        
        Pure structural analysis - generic across all dictionaries.
        Deterministic for a given dictionary.
        """
        # Find all FKs where this category is the child
        child_fks = [
            (c_field, p_cat, p_field)
            for (c_cat, c_field), (p_cat, p_field) in fk_map.items()
            if c_cat == child_cat
        ]
        
        # Must have exactly one FK
        if len(child_fks) != 1:
            return False
        
        # Ensure it's the relationship we're examining
        only_field, only_parent_cat, _ = child_fks[0]
        if only_field != child_field or only_parent_cat != parent_cat:
            return False
        
        pk = primary_keys.get(child_cat)
        
        # No PK → treat as detail table owned by parent
        if not pk:
            return True
        
        # PK exists - check if it's a surrogate key
        if isinstance(pk, str):
            pk_fields = {pk}
        else:
            pk_fields = set(pk)
        
        # Multi-field PK → likely independent entity
        if len(pk_fields) != 1:
            return False
        
        pk_field = next(iter(pk_fields))
        
        # Check if this PK field is referenced as a parent elsewhere
        # If not, it's just a surrogate key for this detail table
        referenced_as_parent = any(
            (p_cat == child_cat and p_field == pk_field)
            for (_, _), (p_cat, p_field) in fk_map.items()
        )
        
        # Not referenced → surrogate key → detail table → ownership
        return not referenced_as_parent
    
    def _is_strong_fk_dependency(
        self,
        child_cat: str,
        child_field: str,
        parent_cat: str,
        parent_field: str,
        primary_keys: Dict[str, Union[str, List[str]]],
        dict_meta: Dict
    ) -> bool:
        """
        STRUCTURAL/SEMANTIC RULE: FK is mandatory and references parent's PK.
        Confidence: MEDIUM (uses mandatory flag + name token matching)
        
        Algorithm:
        1. Verify FK references parent's PK
        2. Check item.mandatory_code in dictionary
        3. Perform token-based name matching (generic, not hardcoded)
        
        Derives ownership from:
        1. FK references parent's primary key (structural)
        2. Field is mandatory in dictionary (structural)
        3. Field name contains a token from parent category name (semantic)
        
        No hardcoded category names - all derived from actual schema.
        Deterministic for a given dictionary.
        """
        # Normalize parent PK
        def norm_pk(pk):
            if pk is None:
                return set()
            if isinstance(pk, str):
                return {pk}
            return set(pk)
        
        pk_parent = norm_pk(primary_keys.get(parent_cat))
        
        # Rule 1: FK must reference parent's PK
        if not pk_parent or parent_field not in pk_parent:
            return False
        
        # Rule 2: Check if field is mandatory in dictionary
        child_item_name = f'_{child_cat}.{child_field}'
        child_item = dict_meta.get('items', {}).get(child_item_name, {})
        
        mandatory = child_item.get('item.mandatory_code', '').strip().lower()
        is_mandatory = mandatory in ['yes', 'y', 'true', '1']
        
        if not is_mandatory:
            # Not mandatory → not a strong dependency
            return False
        
        # Rule 3: Field name must contain meaningful token from parent category
        # Split both names into tokens (by underscores)
        field_tokens = set(child_field.lower().split('_'))
        parent_tokens = set(parent_cat.lower().split('_'))
        
        # Remove common non-semantic tokens
        non_semantic = {'id', 'pdbx', 'auth', 'label'}
        field_tokens -= non_semantic
        parent_tokens -= non_semantic
        
        # Check for token overlap
        if field_tokens & parent_tokens:  # Intersection
            return True
        
        return False
    
    def _apply_semantic_fallbacks(
        self,
        child_cat: str,
        child_field: str,
        parent_cat: str,
        dict_meta: Dict
    ) -> bool:
        """
        Optional semantic heuristics when structural analysis is inconclusive.
        
        These use dictionary descriptions and naming patterns.
        Explicitly separated from core structural logic.
        """
        child_item_name = f'_{child_cat}.{child_field}'
        child_item = dict_meta.get('items', {}).get(child_item_name, {})
        description = child_item.get('item.description', '').lower()
        
        # Check description for ownership indicators
        ownership_terms = ['identifier', 'key', 'belongs', 'member', 'part']
        reference_terms = ['type', 'code', 'symbol', 'class', 'method', 'lookup', 'refers to']
        
        ownership_score = sum(1 for term in ownership_terms if term in description)
        reference_score = sum(1 for term in reference_terms if term in description)
        
        if ownership_score > reference_score:
            return True
        if reference_score > ownership_score:
            return False
        
        # Check category hierarchy
        if parent_cat in child_cat and parent_cat != child_cat:
            return True
        
        return False


class NestingBuilder:
    """Builds nested structure from flat data using relationships"""
    def build_nested_structure(
        self, 
        flat: Dict[str, Any], 
        fk_map: Dict, 
        primary_keys: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build nested structure from flat data"""
        # Identify child-only categories
        child_only_cats = self._identify_child_only_categories(fk_map, flat, primary_keys)
        
        # Create indexed structure
        indexed = self._create_indexed_structure(flat, primary_keys, child_only_cats)
        
        # Assign children to parents
        self._assign_children(indexed, fk_map)
        
        # Build top-level structure
        return self._build_top_level(indexed)

    def _identify_child_only_categories(
        self, 
        fk_map: Dict, 
        flat: Dict[str, Any], 
        primary_keys: Dict[str, Any]
    ) -> Set[str]:
        """Identify categories that are only children with duplicate keys"""
        child_only_cats = set()
        parent_cats = {p for (c, _), (p, _) in fk_map.items()}
        child_cats = {c for (c, _) in fk_map.keys()}
        
        for cat in child_cats:
            if cat not in parent_cats:
                pk_field = primary_keys.get(cat, 'id')
                pk_values = [row.get(pk_field) for row in flat.get(cat, [])]
                if len(pk_values) != len(set(pk_values)):
                    child_only_cats.add(cat)
        return child_only_cats

    def _create_indexed_structure(
        self, 
        flat: Dict[str, Any], 
        primary_keys: Dict[str, Any],
        child_only_cats: Set[str]
    ) -> Dict[str, Any]:
        """Create indexed structure from flat data"""
        indexed = {}
        for entity_name, entity_list in flat.items():
            if entity_name in child_only_cats:
                # Use index as key for child-only categories
                indexed[entity_name] = {str(i): row for i, row in enumerate(entity_list)}
            else:
                # Use primary key for indexing
                pk_field = primary_keys.get(entity_name, 'id')
                entity_dict = {}
                for row in entity_list:
                    pk_value = row.get(pk_field)
                    key = str(pk_value) if pk_value is not None else str(len(entity_dict))
                    entity_dict[key] = row
                indexed[entity_name] = entity_dict
        return indexed

    def _assign_children(
        self, 
        indexed: Dict[str, Any], 
        fk_map: Dict
    ):
        """Assign children to parents using foreign key relationships"""
        for (child_cat, child_col), (parent_cat, _parent_col) in fk_map.items():
            for _child_pk, row in indexed.get(child_cat, {}).items():
                if fk := row.get(child_col):
                    if parent := indexed.get(parent_cat, {}).get(str(fk)):
                        # Ensure nested category names have underscore prefix
                        nested_cat_name = f"_{child_cat}" if not child_cat.startswith("_") else child_cat
                        parent.setdefault(nested_cat_name, []).append(row)

    def _build_top_level(self, indexed: Dict[str, Any]) -> Dict[str, Any]:
        """Build top-level structure from indexed data"""
        actually_nested_cats = self._find_actually_nested_categories(indexed)
        top = {}
        for k, v in indexed.items():
            if k not in actually_nested_cats:
                top[k] = [item for _, item in sorted(v.items())] if isinstance(v, dict) else v
        return top

    def _find_actually_nested_categories(self, indexed: Dict[str, Any]) -> Set[str]:
        """Find categories that are actually nested as children"""
        actually_nested_cats = set()
        for entity_dict in indexed.values():
            for entity_data in entity_dict.values():
                for key in entity_data.keys():
                    # Remove underscore prefix to match indexed keys
                    key_without_prefix = key[1:] if key.startswith('_') else key
                    if key_without_prefix in indexed and isinstance(entity_data.get(key), list):
                        actually_nested_cats.add(key_without_prefix)
        return actually_nested_cats


# ====================== End of File ======================