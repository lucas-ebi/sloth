"""
PDBML Converter - Convert mmCIF to PDBX/PDBML XML format

Optimized for performance with global caching strategy similar to legacy implementation.
"""
import os
import hashlib
import threading
import pickle
import shlex
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple, Set
from xml.etree import ElementTree as ET
from abc import ABC, abstractmethod

from .models import MMCIFDataContainer, Category
from .parser import MMCIFParser
from .validators import XMLSchemaValidator, ValidationError
from .defaults import (
    CacheType, DictDataType, FrameMarker, LoopDataKey, 
    TabularDataCategory, TabularDataField, RelationshipKey, DictItemKey,
    SchemaDataType, TypeSuffix, MappingDataKey,
    # Consolidated classes
    DataValue, DataType, XMLConstant, SemanticPattern, FileOperation
)


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
            container = parser.parse_file(dict_path)
            
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

class XSDParser(MetadataParser):
    """Parses XSD schema files"""
    def __init__(self, cache_manager: CacheManager, quiet: bool = False):
        super().__init__(cache_manager, quiet)
        self.source = None

    def parse(self, xsd_path: Union[str, Path]) -> Dict[str, Any]:
        self.source = xsd_path
        if not xsd_path or not Path(xsd_path).exists():
            return self._empty_schema()
        
        cache_key = self._generate_cache_key(xsd_path)
        cached = self.cache_manager.get(CacheType.XSD.value, cache_key)
        if cached:
            if not self.quiet:
                print("📦 Using cached XSD data")
            return cached
        
        if not self.quiet:
            print("📋 Parsing XSD schema...")
        
        import xml.etree.ElementTree as ET
        tree = ET.parse(xsd_path)
        root = tree.getroot()
        
        parser = XSDSchemaParser()
        complex_types = parser.parse_complex_types(root)
        elements = parser.parse_elements(root, complex_types)
        
        result = {
            SchemaDataType.ELEMENTS.value: elements,
            SchemaDataType.ATTRIBUTES.value: {},  # Could be extended to parse attributes
            SchemaDataType.REQUIRED_ELEMENTS.value: {},  # Could be extended to parse required elements
            SchemaDataType.DEFAULT_VALUES.value: {},  # Could be extended to parse default values
            SchemaDataType.COMPLEX_TYPES.value: complex_types
        }
        
        # Store in unified cache
        self.cache_manager.set(CacheType.XSD.value, cache_key, result)
        if not self.quiet:
            print(f"📋 Parsed {len(elements)} elements, {len(complex_types)} complex types")
        return result

    def _empty_schema(self) -> Dict[str, Any]:
        """Return empty schema structure"""
        return {
            SchemaDataType.ELEMENTS.value: {},
            SchemaDataType.ATTRIBUTES.value: {},
            SchemaDataType.REQUIRED_ELEMENTS.value: {},
            SchemaDataType.DEFAULT_VALUES.value: {},
            SchemaDataType.COMPLEX_TYPES.value: {}
        }

    def _generate_cache_key(self, xsd_path: Union[str, Path]) -> str:
        """Generate cache key based on file path and modification time"""
        xsd_path_resolved = str(Path(xsd_path).resolve())
        mtime = os.path.getmtime(xsd_path)
        return f"xsd_{hashlib.md5(f'{xsd_path_resolved}_{mtime}'.encode()).hexdigest()}"


class XSDSchemaParser:
    """Parses XSD schema content into structured data"""
    NS = {XMLConstant.XS_PREFIX.value: XMLConstant.XS_URI.value}
    
    def parse_complex_types(self, root: ET.Element) -> Dict[str, List[Tuple[str, str]]]:
        """Parse complexType definitions from XSD"""
        complex_types = {}
        for ctype in root.findall(XMLConstant.COMPLEX_TYPE.value, self.NS):
            name = ctype.get(XMLConstant.NAME.value)
            if not name:
                continue
                
            fields = []
            fields.extend(self._parse_attributes(ctype))
            fields.extend(self._parse_sequence_elements(ctype))
            fields.extend(self._parse_direct_elements(ctype))
            
            complex_types[name] = fields
        return complex_types

    def parse_elements(self, root: ET.Element, complex_types: Dict) -> Dict[str, Any]:
        """Parse top-level elements from XSD"""
        elements = {}
        for elem in root.findall(XMLConstant.ELEMENT.value, self.NS):
            table_name = elem.get(XMLConstant.NAME.value)
            type_name = elem.get(XMLConstant.TYPE.value)
            if table_name and type_name:
                # Remove namespace prefix if present
                type_name = type_name.split(':')[-1] if ':' in type_name else type_name
                
                if type_name in complex_types:
                    elements[table_name] = complex_types[type_name]
                else:
                    # Simple element
                    elements[table_name] = [(table_name, type_name)]
        
        # Fallback for schemas without top-level elements
        if not elements:
            elements = self._create_elements_from_complex_types(complex_types)
            
        return elements

    def _parse_attributes(self, parent: ET.Element) -> List[Tuple[str, str]]:
        """Parse attribute definitions from complexType"""
        attributes = []
        for attr in parent.findall(f'.//{XMLConstant.ATTRIBUTE.value}', self.NS):
            attr_name = attr.get(XMLConstant.NAME.value)
            attr_type = attr.get(XMLConstant.TYPE.value, DataType.STRING.value)
            if attr_name:
                attributes.append((attr_name, attr_type))
        return attributes

    def _parse_sequence_elements(self, parent: ET.Element) -> List[Tuple[str, str]]:
        """Parse sequence elements from complexType"""
        elements = []
        sequence = parent.find(f'.//{XMLConstant.SEQUENCE.value}', self.NS)
        if sequence is not None:
            for elem in sequence.findall(XMLConstant.ELEMENT.value, self.NS):
                col_name = elem.get(XMLConstant.NAME.value)
                col_type = self._get_element_type(elem)
                if col_name:
                    elements.append((col_name, col_type))
        return elements

    def _parse_direct_elements(self, parent: ET.Element) -> List[Tuple[str, str]]:
        """Parse direct elements (choice/all) from complexType"""
        elements = []
        for elem in parent.findall(f'.//{XMLConstant.ELEMENT.value}', self.NS):
            col_name = elem.get(XMLConstant.NAME.value)
            col_type = self._get_element_type(elem)
            if col_name:
                elements.append((col_name, col_type))
        return elements

    def _get_element_type(self, elem: ET.Element) -> str:
        """Determine element type handling inline restrictions"""
        col_type = elem.get(XMLConstant.TYPE.value, DataType.STRING.value)
        # Handle inline simpleType with restriction
        if col_type == DataType.STRING.value:
            simple_type = elem.find(f'.//{XMLConstant.RESTRICTION.value}', self.NS)
            if simple_type is not None:
                base_type = simple_type.get(XMLConstant.BASE.value)
                if base_type:
                    col_type = base_type
        return col_type

    def _create_elements_from_complex_types(self, complex_types: Dict) -> Dict[str, Any]:
        """Create elements from complex types when no top-level elements exist"""
        elements = {}
        for type_name, fields in complex_types.items():
            # Convert type names to element names (remove 'Type' suffix)
            elem_name = type_name
            if elem_name.endswith(TypeSuffix.TYPE.value):
                elem_name = elem_name[:-4]
            # Convert camelCase to snake_case for mmCIF compatibility
            elem_name = self._camel_to_snake(elem_name)
            elements[elem_name] = fields
        return elements

    def _camel_to_snake(self, name: str) -> str:
        """Convert camelCase to snake_case"""
        import re
        return re.sub(r'([A-Z])', r'_\1', name).lower().strip('_')

# ====================== Mapping Generator ======================
class MappingGenerator:
    """Generates mapping rules between mmCIF and PDBML formats"""
    def __init__(
        self, 
        dict_parser: DictionaryParser,
        xsd_parser: XSDParser,
        cache_manager: CacheManager,
        quiet: bool = False
    ):
        self.dict_parser = dict_parser
        self.xsd_parser = xsd_parser
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
        xsd_meta = self.xsd_parser.parse(self.xsd_parser.source)
        
        self._mapping_rules = self._generate_mapping(dict_meta, xsd_meta)
        self.cache_manager.set(CacheType.MAPPING_RULES.value, cache_key, self._mapping_rules)
        return self._mapping_rules

    def _generate_cache_key(self) -> str:
        """Generate cache key based on source files and modification times"""
        cache_key_parts = []
        if self.dict_parser.source and Path(self.dict_parser.source).exists():
            dict_path = str(Path(self.dict_parser.source).resolve())
            dict_mtime = os.path.getmtime(self.dict_parser.source)
            cache_key_parts.append(f"dict_{dict_path}_{dict_mtime}")
        if self.xsd_parser.source and Path(self.xsd_parser.source).exists():
            xsd_path = str(Path(self.xsd_parser.source).resolve())
            xsd_mtime = os.path.getmtime(self.xsd_parser.source)
            cache_key_parts.append(f"xsd_{xsd_path}_{xsd_mtime}")
        
        return f"mapping_{hashlib.md5('|'.join(cache_key_parts).encode()).hexdigest()}"

    def _generate_mapping(
        self, 
        dict_meta: Dict[str, Any], 
        xsd_meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate complete mapping rules"""
        builder = MappingBuilder(dict_meta, xsd_meta)
        builder.build_primary_mappings()
        builder.add_xsd_only_categories()
        builder.build_foreign_key_map()
        
        return {
            MappingDataKey.CATEGORY_MAPPING.value: builder.category_mapping,
            MappingDataKey.ITEM_MAPPING.value: builder.item_mapping,
            MappingDataKey.FK_MAP.value: builder.fk_map,
            DictDataType.PRIMARY_KEYS.value: dict_meta.get(DictDataType.PRIMARY_KEYS.value, {})
        }


class MappingBuilder:
    """Builds mapping rules from dictionary and XSD metadata"""
    def __init__(self, dict_meta: Dict[str, Any], xsd_meta: Dict[str, Any]):
        self.dict_meta = dict_meta
        self.xsd_meta = xsd_meta
        self.category_mapping = {}
        self.item_mapping = {}
        self.fk_map = {}
    
    def build_primary_mappings(self):
        """Build primary category and item mappings"""
        for cat_name, cat_data in self.dict_meta[DictDataType.CATEGORIES.value].items():
            self._process_category(cat_name, cat_data)
    
    def _process_category(self, cat_name: str, _cat_data: Dict[str, Any]):
        """Process a single category from dictionary metadata"""
        # Find matching XSD type
        xsd_type_name = f"{cat_name}{TypeSuffix.TYPE.value}"
        xsd_fields = self.xsd_meta[SchemaDataType.COMPLEX_TYPES.value].get(xsd_type_name, [])
        
        # Get all items for this category
        cat_items = self._get_category_items(cat_name)
        
        # Combine XSD fields and dictionary items
        all_fields = self._combine_fields(xsd_fields, cat_items)
        
        # Create category mapping
        self.category_mapping[cat_name] = {MappingDataKey.FIELDS.value: sorted(list(all_fields))}
        
        # Map individual items
        self.item_mapping[cat_name] = {}
        for field_name in all_fields:
            self._map_item(cat_name, field_name, xsd_fields)
    
    def _get_category_items(self, cat_name: str) -> Set[str]:
        """Get all item names for a category"""
        cat_items = set()
        for item_name in self.dict_meta[DictDataType.ITEMS.value]:
            if item_name.startswith(f"{FrameMarker.UNDERSCORE.value}{cat_name}{DataValue.DOT.value}"):
                field_name = item_name[len(f"{FrameMarker.UNDERSCORE.value}{cat_name}{DataValue.DOT.value}"):]
                cat_items.add(field_name)
        return cat_items
    
    def _combine_fields(self, xsd_fields: List[Tuple[str, str]], cat_items: Set[str]) -> Set[str]:
        """Combine XSD fields and dictionary items"""
        all_fields = set(field_name for field_name, _ in xsd_fields)
        all_fields.update(cat_items)
        return all_fields
    
    def _map_item(self, cat_name: str, field_name: str, xsd_fields: List[Tuple[str, str]]):
        """Map a single item from dictionary and XSD metadata"""
        item_name = f"_{cat_name}.{field_name}"
        item_data = self.dict_meta['items'].get(item_name, {})
        
        # Determine field type
        field_type = next(
            (ft for fn, ft in xsd_fields if fn == field_name), 
            item_data.get("item_type.code", "xs:string")
        )
        
        # Create item mapping
        self.item_mapping[cat_name][field_name] = {
            "type": field_type,
            "enum": self.dict_meta['enumerations'].get(item_name),
            "description": item_data.get("item_description.description", "")
        }
    
    def add_xsd_only_categories(self):
        """Add categories that only exist in XSD schema"""
        for xsd_type_name, xsd_fields in self.xsd_meta['complex_types'].items():
            if not xsd_type_name.endswith('Type'):
                continue
                
            cat_name = xsd_type_name[:-4]  # Remove 'Type' suffix
            snake_name = self._camel_to_snake(cat_name)
            
            # Check if category already exists
            for possible_name in [cat_name, snake_name]:
                if possible_name in self.category_mapping:
                    break
                if possible_name not in self.dict_meta['categories']:
                    self._add_xsd_category(possible_name, xsd_fields)
                    break
    
    def _add_xsd_category(self, cat_name: str, xsd_fields: List[Tuple[str, str]]):
        """Add an XSD-only category to mappings"""
        self.category_mapping[cat_name] = {
            "fields": [field_name for field_name, _ in xsd_fields]
        }
        self.item_mapping[cat_name] = {}
        for field_name, field_type in xsd_fields:
            self.item_mapping[cat_name][field_name] = {
                "type": field_type,
                "enum": None,
                "description": ""
            }
    
    def _camel_to_snake(self, name: str) -> str:
        """Convert camelCase to snake_case"""
        import re
        return re.sub(r'([A-Z])', r'_\1', name).lower().strip('_')
    
    def build_foreign_key_map(self):
        """Build foreign key mapping from relationships"""
        for rel in self.dict_meta['relationships']:
            self._process_relationship(rel)
    
    def _process_relationship(self, rel: Dict[str, Any]):
        """Process a single relationship entry"""
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
            self.fk_map[(child_cat, child_field)] = (parent_cat, parent_field)
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
            self.fk_map[(child_parts[0], child_parts[1])] = (parent_parts[0], parent_parts[1])


# ====================== PDBML Converter ======================
class PDBMLConverter:
    """Converts mmCIF data to PDBML XML format"""
    def __init__(
        self, 
        mapping_generator: MappingGenerator,
        permissive: bool = False,
        quiet: bool = False
    ):
        self.mapping_generator = mapping_generator
        self.permissive = permissive
        self.quiet = quiet
        self.namespace = XMLConstant.get_default_namespace()
        self.field_resolver = FieldTypeResolver(mapping_generator, quiet)
        self.xml_generator = XMLGenerator(self.field_resolver, quiet)

    def convert_to_pdbml(self, mmcif_container: MMCIFDataContainer) -> str:
        """Convert mmCIF container to PDBML XML string"""
        return self.xml_generator.convert(mmcif_container)


class FieldTypeResolver:
    """Resolves field types and XML representation types"""
    def __init__(self, mapping_generator: MappingGenerator, quiet: bool = False):
        self.mapping_generator = mapping_generator
        self.quiet = quiet
        # Get schema info once at init
        self.xsd_meta = self.mapping_generator.xsd_parser.parse(
            self.mapping_generator.xsd_parser.source
        )
        # Pre-compute attribute/element mappings
        self._attribute_fields_cache = {}
        self._precompute_attribute_fields()

    def _precompute_attribute_fields(self):
        """Pre-compute attribute/element mappings for all categories"""
        tree = self._get_xsd_tree()
        if tree is None:
            return
            
        ns = {'xs': 'http://www.w3.org/2001/XMLSchema'}
        root = tree.getroot()
        
        for type_elem in root.findall(".//xs:complexType", ns):
            type_name = type_elem.get('name')
            if not type_name or not type_name.endswith('Type'):
                continue
                
            cat_name = type_name[:-4]
            if cat_name not in self._attribute_fields_cache:
                self._attribute_fields_cache[cat_name] = {'attributes': set(), 'elements': set()}
            
            # Collect attributes
            for attr_elem in type_elem.findall(".//xs:attribute", ns):
                if attr_name := attr_elem.get('name'):
                    self._attribute_fields_cache[cat_name]['attributes'].add(attr_name)
            
            # Collect elements  
            for elem_elem in type_elem.findall(".//xs:element", ns):
                if elem_name := elem_elem.get('name'):
                    self._attribute_fields_cache[cat_name]['elements'].add(elem_name)

    def _get_xsd_tree(self):
        """Get cached XSD tree"""
        xsd_path = self.mapping_generator.xsd_parser.source
        if not xsd_path or not Path(xsd_path).exists():
            return None
            
        # Generate cache key
        xsd_path_resolved = str(Path(xsd_path).resolve())
        mtime = os.path.getmtime(xsd_path)
        cache_key = f"xsd_tree_{hashlib.md5(f'{xsd_path_resolved}_{mtime}'.encode()).hexdigest()}"
        
        # Check cache
        cached_tree = self.mapping_generator.cache_manager.get('xsd_trees', cache_key)
        if cached_tree:
            return cached_tree
        
        # Parse and cache
        try:
            tree = ET.parse(xsd_path)
            self.mapping_generator.cache_manager.set('xsd_trees', cache_key, tree)
            return tree
        except Exception:
            return None

    def get_field_type(self, cat_name: str, field_name: str) -> str:
        """Get the XSD type for a field"""
        type_name = f"{cat_name}{TypeSuffix.TYPE.value}"
        if type_name in self.xsd_meta[SchemaDataType.COMPLEX_TYPES.value]:
            for fn, ft in self.xsd_meta[SchemaDataType.COMPLEX_TYPES.value][type_name]:
                if fn == field_name:
                    return ft
        return DataType.STRING.value  # Default to string

    def is_typed_field(self, field_type: str) -> bool:
        """Check if a field has a specific non-string type"""
        typed_fields = [DataType.INTEGER.value, DataType.XSD_INT.value, DataType.DECIMAL.value, DataType.DOUBLE.value, 
                       DataType.XSD_FLOAT.value, DataType.XSD_BOOLEAN.value, DataType.XSD_DATE.value, DataType.XSD_DATETIME.value]
        return field_type in typed_fields

    def is_attribute_field(self, cat_name: str, field_name: str) -> bool:
        """Determine if a field should be an XML attribute"""
        # Check if primary key
        mapping = self.mapping_generator.get_mapping_rules()
        primary_keys = mapping.get(DictDataType.PRIMARY_KEYS.value, {})
        if cat_name in primary_keys:
            pk = primary_keys[cat_name]
            if isinstance(pk, list):
                if field_name in pk:
                    return True
            elif field_name == pk:
                return True
        
        # Check pre-computed mappings
        if cat_name in self._attribute_fields_cache:
            cache_entry = self._attribute_fields_cache[cat_name]
            if field_name in cache_entry[SchemaDataType.ATTRIBUTES.value]:
                return True
            if field_name in cache_entry[SchemaDataType.ELEMENTS.value]:
                return False
        
        # Fallback to pattern matching
        attr_patterns = [SemanticPattern.ID.value, SemanticPattern.NAME.value, SemanticPattern.TYPE.value, SemanticPattern.VALUE.value, SemanticPattern.CODE.value]
        return (field_name in attr_patterns or 
               field_name.endswith(SemanticPattern.ID_SUFFIX.value) or 
               field_name.endswith(SemanticPattern.NO_SUFFIX.value) or 
               field_name.endswith(SemanticPattern.INDEX_SUFFIX.value))


class XMLGenerator:
    """Generates PDBML XML from mmCIF data"""
    def __init__(self, field_resolver: FieldTypeResolver, quiet: bool = False):
        self.field_resolver = field_resolver
        self.quiet = quiet

    def convert(self, mmcif_container: MMCIFDataContainer) -> str:
        """Convert mmCIF container to PDBML XML string"""
        mapping = self.field_resolver.mapping_generator.get_mapping_rules()
        block = next(iter(mmcif_container))
        
        root = ET.Element('datablock')
        root.set(XMLConstant.XMLNS.value, XMLConstant.PDBX_V50.value)
        root.set(f'{{{XMLConstant.XSI_URI.value}}}{XMLConstant.SCHEMA_LOCATION.value}', 
                f'{XMLConstant.PDBX_V50.value} {XMLConstant.PDBX_V50_XSD.value}')
        root.set(XMLConstant.DATABLOCK_NAME.value, block.name)
        
        for cat_name, category in block.data.items():
            cat_name_clean = cat_name.lstrip("_")
            if category.row_count == 0:
                continue
                
            if cat_name_clean not in mapping[MappingDataKey.CATEGORY_MAPPING.value]:
                if not self.quiet:
                    print(f"Warning: No mapping found for category {cat_name_clean}")
                continue
            
            self._create_category_element(root, cat_name_clean, category, mapping)
        
        xml_string = ET.tostring(root, encoding=XMLConstant.ENCODING.value).decode(XMLConstant.ENCODING.value)
        return XMLConstant.XML_VERSION.value + xml_string

    def _create_category_element(self, parent: ET.Element, cat_name: str, 
                               category: Category, mapping: Dict[str, Any]) -> ET.Element:
        """Create XML elements for a category"""
        category_elem = ET.SubElement(parent, f'{cat_name}{TypeSuffix.CATEGORY.value}')
        mapped_fields = mapping[MappingDataKey.CATEGORY_MAPPING.value][cat_name][MappingDataKey.FIELDS.value]
        
        for i in range(category.row_count):
            row = category[i]
            row_elem = ET.SubElement(category_elem, cat_name)
            self._add_row_data(row_elem, row, cat_name, mapped_fields)
        
        return category_elem

    def _add_row_data(self, row_elem: ET.Element, row: Any, cat_name: str, mapped_fields: List[str]):
        """Add data from a single row to XML elements"""
        for field in mapped_fields:
            value = row.data.get(field)
            if value is None:
                continue
                
            clean_value = self._clean_value(value)
            
            if self.field_resolver.is_attribute_field(cat_name, field):
                self._add_attribute(row_elem, field, clean_value)
            else:
                self._add_element(row_elem, field, clean_value, cat_name)

    def _add_attribute(self, element: ET.Element, name: str, value: str):
        """Add an XML attribute if value is valid"""
        if value and value not in [DataValue.EMPTY_STRING.value, DataValue.DOT.value, DataValue.QUESTION_MARK.value]:
            element.set(name, value)

    def _add_element(self, parent: ET.Element, name: str, value: str, cat_name: str):
        """Add an XML element with proper value handling"""
        if value in [DataValue.EMPTY_STRING.value, DataValue.DOT.value, DataValue.QUESTION_MARK.value]:
            self._handle_missing_value(parent, name, cat_name)
        else:
            field_elem = ET.SubElement(parent, name)
            field_elem.text = value

    def _handle_missing_value(self, parent: ET.Element, name: str, cat_name: str):
        """Handle missing values based on field type"""
        field_type = self.field_resolver.get_field_type(cat_name, name)
        field_elem = ET.SubElement(parent, name)
        
        if self.field_resolver.is_typed_field(field_type):
            if 'integer' in field_type.lower() or 'int' in field_type.lower():
                field_elem.text = DataValue.DEFAULT_INTEGER.value
            elif 'decimal' in field_type.lower() or 'double' in field_type.lower() or 'float' in field_type.lower():
                field_elem.text = DataValue.DEFAULT_DECIMAL.value
            else:
                field_elem.text = DataValue.EMPTY_STRING.value
        else:
            field_elem.text = DataValue.EMPTY_STRING.value

    def _clean_value(self, value: Any) -> str:
        """Clean and normalize values from mmCIF data"""
        if value is None:
            return DataValue.EMPTY_STRING.value
        
        str_value = str(value)
        if len(str_value) >= 2:
            if (str_value.startswith(FileOperation.DOUBLE_QUOTE.value) and str_value.endswith(FileOperation.DOUBLE_QUOTE.value)) or \
               (str_value.startswith(FileOperation.SINGLE_QUOTE.value) and str_value.endswith(FileOperation.SINGLE_QUOTE.value)):
                return str_value[1:-1]
        return str_value


# ====================== Relationship Resolver ======================
class RelationshipResolver:
    """Resolves entity relationships for nested JSON output"""
    def __init__(self, mapping_generator: MappingGenerator):
        self.mapping_generator = mapping_generator
        self.ownership_analyzer = OwnershipAnalyzer(mapping_generator)
        self.nesting_builder = NestingBuilder()
        
    @property
    def mapping_rules(self) -> Dict[str, Any]:
        """Cached access to mapping rules"""
        return self.mapping_generator.get_mapping_rules()

    def resolve_relationships(self, xml_content: str) -> Dict[str, Any]:
        # Parse XML to flat dict
        flattener = XMLFlattener()
        flat = flattener.flatten(xml_content)
        
        # Get mapping rules
        mapping = self.mapping_rules
        fk_map = mapping["fk_map"]
        primary_keys = mapping.get("primary_keys", {})
        
        # Filter FK map to only include ownership relationships
        ownership_fk_map = self.ownership_analyzer.filter_ownership_relationships(fk_map, flat)
        
        # Build nested structure
        return self.nesting_builder.build_nested_structure(flat, ownership_fk_map, primary_keys)


class XMLFlattener:
    """Flattens XML content into a dictionary structure"""
    def flatten(self, xml_content: str) -> Dict[str, Any]:
        """Convert XML to flat dictionary of entities and rows"""
        tree = ET.ElementTree(ET.fromstring(xml_content))
        root = tree.getroot()
        flat = {}
        
        for elem in root:
            # Strip namespace from tag name 
            tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            
            # Remove 'Category' suffix to get entity name
            entity_name = tag.replace('Category', '') if tag.endswith('Category') else tag
            
            # Process each item in the category
            for item_elem in elem:
                row_data = self._extract_row_data(item_elem)
                flat.setdefault(entity_name, []).append(row_data)
        
        return flat

    def _extract_row_data(self, elem: ET.Element) -> Dict[str, Any]:
        """Extract row data from XML element"""
        row_data = {}
        # Add attributes
        row_data.update(elem.attrib)
        # Add child elements
        for child in elem:
            child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
            row_data[child_tag] = child.text
        return row_data


class OwnershipAnalyzer:
    """Analyzes relationships to determine ownership"""
    def __init__(self, mapping_generator: MappingGenerator):
        self.mapping_generator = mapping_generator

    def filter_ownership_relationships(self, fk_map: Dict, data: Dict) -> Dict:
        """Filter FK map to include only ownership relationships"""
        # Get dictionary metadata for relationship analysis
        dict_meta = self.mapping_generator.dict_parser.parse(
            self.mapping_generator.dict_parser.source
        )
        
        ownership_fk_map = {}
        
        for (child_cat, child_field), (parent_cat, parent_field) in fk_map.items():
            if self._is_ownership_relationship(
                child_cat, child_field, parent_cat, parent_field, 
                dict_meta, data
            ):
                ownership_fk_map[(child_cat, child_field)] = (parent_cat, parent_field)
        
        return ownership_fk_map
    
    def _is_ownership_relationship(
        self, 
        child_cat: str, 
        child_field: str, 
        parent_cat: str, 
        parent_field: str,
        dict_meta: Dict,
        data: Dict
    ) -> bool:
        """Determine if a relationship represents ownership"""
        # Check for explicit indicators in dictionary metadata
        for rel in dict_meta.get('relationships', []):
            if self._matches_relationship(rel, child_cat, child_field, parent_cat, parent_field):
                if self._has_ownership_indicators(rel):
                    return True
                if self._has_reference_indicators(rel):
                    return False
        
        # Analyze relationship characteristics
        cardinality_score = self._analyze_cardinality(
            child_cat, child_field, parent_cat, parent_field, data
        )
        semantic_score = self._analyze_semantic_patterns(
            child_cat, child_field, parent_cat, dict_meta
        )
        hierarchy_score = self._analyze_category_hierarchy(
            child_cat, parent_cat, dict_meta
        )
        
        return cardinality_score + semantic_score + hierarchy_score > 0
    
    def _matches_relationship(self, rel: Dict, child_cat: str, child_field: str, 
                            parent_cat: str, parent_field: str) -> bool:
        """Check if relationship metadata matches current relationship"""
        rel_child_name = rel.get('child_name', '').strip('_')
        rel_parent_name = rel.get('parent_name', '').strip('_')
        return (rel_child_name.endswith(f'{child_cat}.{child_field}') and 
               rel_parent_name.endswith(f'{parent_cat}.{parent_field}'))
    
    def _has_ownership_indicators(self, rel: Dict) -> bool:
        """Check for ownership indicators in relationship metadata"""
        description = rel.get('description', '').lower()
        ownership_terms = ['belongs to', 'owned by', 'part of', 'contained in', 'member of']
        return any(term in description for term in ownership_terms)
    
    def _has_reference_indicators(self, rel: Dict) -> bool:
        """Check for reference indicators in relationship metadata"""
        description = rel.get('description', '').lower()
        reference_terms = ['refers to', 'references', 'lookup', 'type of', 'code for']
        return any(term in description for term in reference_terms)
    
    def _analyze_cardinality(
        self, 
        child_cat: str, 
        child_field: str, 
        parent_cat: str, 
        _parent_field: str, 
        data: Dict
    ) -> float:
        """Analyze relationship cardinality from actual data"""
        if child_cat not in data or parent_cat not in data:
            return 0.0
        
        child_data = data[child_cat]
        parent_data = data[parent_cat]
        
        if not child_data or not parent_data:
            return 0.0
        
        # Check for lookup/reference table patterns
        lookup_table_patterns = [
            SemanticPattern.TYPE.value, SemanticPattern.CLASS.value, SemanticPattern.METHOD.value, 
            SemanticPattern.STATUS.value, SemanticPattern.CODE.value, SemanticPattern.SYMBOL.value,
            SemanticPattern.ENUM.value, SemanticPattern.DICT.value, SemanticPattern.LIST.value, 
            SemanticPattern.TABLE.value, SemanticPattern.REF.value
        ]
        if (any(pattern in parent_cat.lower() for pattern in lookup_table_patterns) or
            any(pattern in child_field.lower() for pattern in lookup_table_patterns)):
            return -30.0
        
        # Analyze parent-child relationships
        parent_to_children = {}
        for child_row in child_data:
            if fk_value := child_row.get(child_field):
                parent_to_children.setdefault(fk_value, 0)
                parent_to_children[fk_value] += 1
        
        if not parent_to_children:
            return 0.0
        
        # Calculate coverage ratio
        parent_count = len(parent_data)
        referenced_parent_count = len(parent_to_children)
        coverage_ratio = referenced_parent_count / parent_count if parent_count > 0 else 0
        if coverage_ratio < 0.3:
            return -20.0
        
        # Calculate average children per parent
        avg_children = sum(parent_to_children.values()) / len(parent_to_children)
        if avg_children > 10:
            return -15.0
        elif avg_children > 5:
            return -5.0
        elif avg_children > 2:
            return 10.0
        else:
            return 20.0
    
    def _analyze_semantic_patterns(
        self, 
        child_cat: str, 
        child_field: str, 
        parent_cat: str, 
        dict_meta: Dict
    ) -> float:
        """Analyze semantic naming patterns from dictionary item definitions"""
        child_item_name = f'_{child_cat}.{child_field}'
        child_item = dict_meta.get('items', {}).get(child_item_name, {})
        description = child_item.get('item.description', '').lower()
        
        semantic_score = 0.0
        
        # Strong reference indicators
        strong_reference_patterns = [
            SemanticPattern.TYPE_SYMBOL.value, SemanticPattern.SYMBOL.value, SemanticPattern.TYPE.value, 
            SemanticPattern.CODE.value, SemanticPattern.CLASS.value, SemanticPattern.METHOD.value, 
            SemanticPattern.STATUS.value, SemanticPattern.ENUM.value, SemanticPattern.CATEGORY.value, 
            SemanticPattern.KIND.value
        ]
        if any(pattern in child_field.lower() for pattern in strong_reference_patterns):
            semantic_score -= 40
        
        # Strong ownership indicators
        ownership_patterns = [f'{parent_cat}{SemanticPattern.ID_SUFFIX.value}', SemanticPattern.ASYM_ID.value, 
                             SemanticPattern.ENTITY_ID.value, SemanticPattern.STRUCT_ID.value]
        if any(pattern in child_field.lower() for pattern in ownership_patterns):
            semantic_score += 30
        
        # Primary key references
        if child_field == SemanticPattern.ID.value or child_field.endswith(SemanticPattern.ID_SUFFIX.value):
            if parent_cat in child_field or child_field == f'{parent_cat}{SemanticPattern.ID_SUFFIX.value}':
                semantic_score += 30
        
        # Category name inclusion
        if parent_cat in child_cat:
            semantic_score += 20
        
        # Field description analysis
        if description:
            ownership_terms = ['identifier', 'key', 'belongs', 'member', 'part']
            reference_terms = ['type', 'code', 'symbol', 'class', 'method', 'lookup', 'refers to']
            for term in ownership_terms:
                if term in description:
                    semantic_score += 10
            for term in reference_terms:
                if term in description:
                    semantic_score -= 15
        
        return semantic_score
    
    def _analyze_category_hierarchy(
        self, 
        child_cat: str, 
        parent_cat: str, 
        dict_meta: Dict
    ) -> float:
        """Analyze category hierarchy patterns from dictionary metadata"""
        child_category = dict_meta.get('categories', {}).get(child_cat, {})
        parent_category = dict_meta.get('categories', {}).get(parent_cat, {})
        hierarchy_score = 0.0
        
        # Category descriptions
        child_desc = child_category.get('category.description', '').lower()
        parent_desc = parent_category.get('category.description', '').lower()
        
        # Naming patterns
        if child_cat.startswith(parent_cat):
            hierarchy_score += 25
        
        # Description terms
        if child_desc and parent_desc:
            if 'detail' in child_desc or 'specific' in child_desc:
                hierarchy_score += 15
            if 'general' in parent_desc or 'summary' in parent_desc:
                hierarchy_score += 10
        
        # Structural patterns
        structural_patterns = [
            (child_cat.endswith('_site'), parent_cat.endswith('_asym')),
            (child_cat.endswith('_atom'), parent_cat.endswith('_residue')),
            ('author' in child_cat, 'label' in parent_cat),
        ]
        for child_pattern, parent_pattern in structural_patterns:
            if child_pattern and parent_pattern:
                hierarchy_score += 20
                break
        
        return hierarchy_score


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
                        parent.setdefault(child_cat, []).append(row)

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
                    if key in indexed and isinstance(entity_data.get(key), list):
                        actually_nested_cats.add(key)
        return actually_nested_cats


# ====================== End of File ======================