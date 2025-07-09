"""
PDBML Converter - Convert mmCIF to PDBX/PDBML XML format

Optimized for performance with global caching strategy similar to legacy implementation.
"""

import os
import re
import json
import hashlib
import threading
import traceback
import pickle
import shlex
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from xml.etree import ElementTree
from xml.etree import ElementTree as ET
from functools import lru_cache, wraps
from abc import ABC, abstractmethod

from .models import MMCIFDataContainer, DataBlock, Category
from .parser import MMCIFParser
from .validators import XMLSchemaValidator, ValidationError
from .schemas import (
    XMLLocation, XMLElementType, XMLGroupingType, XMLContainerType,
    PDBMLElement, PDBMLAttribute, DebugFile, get_numeric_fields, 
    is_null_value, PDBMLNamespace
)

# ====================== Unified High-Performance Caching ======================
# Global caches for maximum performance (similar to legacy implementation)
_GLOBAL_CACHES = {
    'dictionary': {},
    'xsd': {},
    'mapping_rules': {},
    'xsd_trees': {}
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
        cache_file = self.cache_dir / f"{cache_type}_{key}.pkl"
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'rb') as f:
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
        cache_file = self.cache_dir / f"{cache_type}_{key}.pkl"
        try:
            with open(cache_file, 'wb') as f:
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
            return {
                'categories': {},
                'items': {},
                'relationships': [],
                'enumerations': {},
                'item_types': {}
            }
        
        # Generate cache key with path and modification time for auto-invalidation
        dict_path_resolved = str(Path(dict_path).resolve())
        mtime = os.path.getmtime(dict_path)
        cache_key = f"dict_{hashlib.md5(f'{dict_path_resolved}_{mtime}'.encode()).hexdigest()}"
        
        # Check unified cache
        cached = self.cache_manager.get('dictionary', cache_key)
        if cached:
            if not self.quiet:
                print("📦 Using cached dictionary data")
            return cached
        
        if not self.quiet:
            print("📚 Parsing dictionary...")
        
        # Parse the mmCIF dictionary using save frames
        categories = {}
        items = {}
        relationships = []
        enumerations = {}
        item_types = {}
        
        # Read file and parse save frames
        with open(dict_path, 'r') as f:
            content = f.read()
        
        # Split into save frames
        import re
        frames = re.split(r'\nsave_', content)
        
        for frame_content in frames[1:]:  # Skip first part (global data)
            lines = frame_content.strip().split('\n')
            if not lines:
                continue
                
            frame_name = lines[0].strip()
            frame_data = {}
            
            # Simple state machine for parsing
            i = 1
            while i < len(lines):
                line = lines[i].strip()
                
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    i += 1
                    continue
                
                # End of save frame
                if line == 'save_':
                    break
                
                # Handle multiline text blocks
                if line.startswith('_') and i + 1 < len(lines) and lines[i + 1].strip() == ';':
                    # This is a key followed by multiline content
                    key = line.strip('_')
                    i += 2  # Skip the key line and the opening ';'
                    
                    # Collect multiline content until closing ';'
                    multiline_content = []
                    while i < len(lines):
                        if lines[i].strip() == ';':
                            break
                        multiline_content.append(lines[i])
                        i += 1
                    
                    frame_data[key] = '\n'.join(multiline_content).strip()
                    i += 1  # Skip the closing ';'
                    continue
                
                # Handle simple key-value pairs
                if line.startswith('_'):
                    parts = line.split(None, 1)
                    key = parts[0].strip('_')
                    if len(parts) == 2:
                        value = parts[1].strip().strip('"\'')
                        frame_data[key] = value
                    else:
                        frame_data[key] = ''
                    i += 1
                    continue
                
                # Handle loops
                if line == 'loop_':
                    i += 1
                    # Collect loop headers
                    loop_headers = []
                    while i < len(lines) and lines[i].strip().startswith('_'):
                        loop_headers.append(lines[i].strip())
                        i += 1
                    
                    # Collect loop data
                    loop_data = []
                    while i < len(lines):
                        line = lines[i].strip()
                        if not line or line.startswith('#') or line == 'save_' or line.startswith('_') or line == 'loop_':
                            break
                        
                        # Parse the data line
                        import shlex
                        try:
                            row_data = shlex.split(line)
                            loop_data.append(row_data)
                        except ValueError:
                            row_data = line.split()
                            loop_data.append(row_data)
                        i += 1
                    
                    # Process loop data - handle multiple rows properly
                    if loop_data:
                        # For loops, we need to handle multiple items being defined
                        # Each row in the loop defines a separate item/entity
                        loop_items = []
                        for row in loop_data:
                            if len(row) >= len(loop_headers):
                                row_data = {}
                                for j, header in enumerate(loop_headers):
                                    key = header.strip('_')
                                    row_data[key] = row[j].strip('"\'')
                                loop_items.append(row_data)
                        
                        # Store loop data for later processing
                        frame_data['_loop_data'] = {
                            'headers': [h.strip('_') for h in loop_headers],
                            'items': loop_items
                        }
                        
                        # Also set first item's data as direct attributes for compatibility
                        if loop_items:
                            first_item = loop_items[0]
                            for key, value in first_item.items():
                                if key not in frame_data:
                                    frame_data[key] = value
                    continue
                
                # Skip unrecognized lines
                i += 1
            
            # Process loop data first if present
            if '_loop_data' in frame_data:
                loop_info = frame_data['_loop_data']
                headers = loop_info['headers']
                
                # Process each item in the loop
                for loop_item in loop_info['items']:
                    # Create a combined data structure for each loop item
                    combined_data = {**frame_data}  # Start with frame data
                    combined_data.update(loop_item)  # Add loop item data
                    
                    # Classify each loop item
                    if 'category.id' in combined_data:
                        categories[combined_data['category.id']] = combined_data
                    elif 'item.name' in combined_data:
                        item_name = combined_data['item.name'].strip('"\'')
                        items[item_name] = combined_data
                        
                        # Check for enumerations in loop items
                        if 'item_enumeration.value' in combined_data:
                            values = combined_data['item_enumeration.value']
                            if isinstance(values, str):
                                values = [values]
                            enumerations[item_name] = values
                    elif 'item_linked.child_name' in combined_data and 'item_linked.parent_name' in combined_data:
                        relationships.append(combined_data)
                    elif 'pdbx_item_linked_group_list.child_category_id' in combined_data:
                        # Handle pdbx_item_linked_group_list entries
                        child_cat = combined_data.get('pdbx_item_linked_group_list.child_category_id')
                        child_name = combined_data.get('pdbx_item_linked_group_list.child_name')
                        parent_name = combined_data.get('pdbx_item_linked_group_list.parent_name')
                        parent_cat = combined_data.get('pdbx_item_linked_group_list.parent_category_id')
                        
                        if child_cat and child_name and parent_name and parent_cat:
                            relationships.append({
                                'child_category': child_cat,
                                'child_name': child_name,
                                'parent_category': parent_cat,
                                'parent_name': parent_name
                            })
            else:
                # Classify frame by type (non-loop items)
                if 'category.id' in frame_data:
                    categories[frame_data['category.id']] = frame_data
                elif 'item.name' in frame_data:
                    item_name = frame_data['item.name'].strip('"\'')
                    items[item_name] = frame_data
                    
                    # Check for enumerations
                    if 'item_enumeration.value' in frame_data:
                        values = frame_data['item_enumeration.value']
                        if isinstance(values, str):
                            values = [values]
                        enumerations[item_name] = values
                elif 'item_linked.child_name' in frame_data and 'item_linked.parent_name' in frame_data:
                    relationships.append(frame_data)
                elif 'pdbx_item_linked_group_list.child_category_id' in frame_data:
                    # Handle pdbx_item_linked_group_list entries
                    child_cat = frame_data.get('pdbx_item_linked_group_list.child_category_id')
                    child_name = frame_data.get('pdbx_item_linked_group_list.child_name')
                    parent_name = frame_data.get('pdbx_item_linked_group_list.parent_name')
                    parent_cat = frame_data.get('pdbx_item_linked_group_list.parent_category_id')
                    
                    if child_cat and child_name and parent_name and parent_cat:
                        relationships.append({
                            'child_category': child_cat,
                            'child_name': child_name,
                            'parent_category': parent_cat,
                            'parent_name': parent_name
                        })
        
        # Also parse any tabular data from the main parser
        try:
            from .parser import MMCIFParser  # Explicit import
            parser = MMCIFParser()
            container = parser.parse_file(dict_path)
            
            # Add item type information from tables
            if "item_type_list" in container[0].data:
                type_list = container[0].data["item_type_list"]
                for i in range(type_list.row_count):
                    row = type_list[i].data
                    code = row.get("code")
                    if code:
                        item_types[code] = row
            
            # Extract relationships from pdbx_item_linked_group_list
            if "pdbx_item_linked_group_list" in container[0].data:
                linked_list = container[0].data["pdbx_item_linked_group_list"]
                if not self.quiet:
                    print(f"📊 Found {linked_list.row_count} relationships in dictionary")
                for i in range(linked_list.row_count):
                    row = linked_list[i].data
                    child_cat = row.get("child_category_id")
                    child_name = row.get("child_name", "").strip('"')
                    parent_name = row.get("parent_name", "").strip('"')
                    parent_cat = row.get("parent_category_id")
                    
                    if child_cat and child_name and parent_name and parent_cat:
                        relationships.append({
                            'child_category': child_cat,
                            'child_name': child_name,
                            'parent_category': parent_cat,
                            'parent_name': parent_name
                        })
        except Exception as e:
            if not self.quiet:
                print(f"Warning: Could not parse tabular data: {e}")
                import traceback
                traceback.print_exc()
        
        # Extract primary key information
        primary_keys = {}
        for cat_name, cat_data in categories.items():
            # Handle both single and composite keys
            key_items = []
            
            # Check if we have a direct key field
            if 'category_key.name' in cat_data:
                key_item = cat_data['category_key.name'].strip('"\'')
                if key_item:
                    key_items.append(key_item)
            
            # Check for composite keys in _loop_data
            if '_loop_data' in cat_data:
                loop_data = cat_data['_loop_data']
                for item in loop_data['items']:
                    if 'category_key.name' in item:
                        key_item = item['category_key.name'].strip('"\'')
                        if key_item and key_item not in key_items:
                            key_items.append(key_item)
            
            # Process all found key items
            if key_items:
                fields = []
                for key_item in key_items:
                    if key_item.startswith('_') and '.' in key_item:
                        # Extract field name from "_category.field" format
                        field_name = key_item.split('.')[-1]
                        fields.append(field_name)
                if fields:
                    primary_keys[cat_name] = fields[0] if len(fields) == 1 else fields

        result = {
            "categories": categories,
            "items": items,
            "relationships": relationships,
            "enumerations": enumerations,
            "item_types": item_types,
            "primary_keys": primary_keys
        }
        
        # Debug output
        if not self.quiet:
            print(f"📚 Parsed {len(categories)} categories, {len(items)} items")
            print(f"📝 Found {len(primary_keys)} primary keys:")
            for cat, key in primary_keys.items():
                print(f"  - {cat}: {key}")
        
        # Store in unified cache
        self.cache_manager.set('dictionary', cache_key, result)
        return result

class XSDParser(MetadataParser):
    """Parses XSD schema files"""
    def __init__(self, cache_manager: CacheManager, quiet: bool = False):
        super().__init__(cache_manager, quiet)
        self.source = None

    def parse(self, xsd_path: Union[str, Path]) -> Dict[str, Any]:
        self.source = xsd_path
        if not xsd_path or not Path(xsd_path).exists():
            return {
                'elements': {},
                'attributes': {},
                'required_elements': {},
                'default_values': {},
                'complex_types': {}
            }
        
        # Generate cache key with path and modification time for auto-invalidation
        xsd_path_resolved = str(Path(xsd_path).resolve())
        mtime = os.path.getmtime(xsd_path)
        cache_key = f"xsd_{hashlib.md5(f'{xsd_path_resolved}_{mtime}'.encode()).hexdigest()}"
        
        # Check unified cache
        cached = self.cache_manager.get('xsd', cache_key)
        if cached:
            if not self.quiet:
                print("📦 Using cached XSD data")
            return cached
        if not self.quiet:
            print("📋 Parsing XSD schema...")
        import xml.etree.ElementTree as ET
        ns = {'xs': 'http://www.w3.org/2001/XMLSchema'}
        tree = ET.parse(xsd_path)
        root = tree.getroot()
        
        def _get_element_type(elem, ns):
            """Helper function to extract type from an element, handling inline restrictions"""
            col_type = elem.get('type', 'xs:string')
            # If no direct type, check for inline simpleType with restriction
            if col_type == 'xs:string':
                simple_type = elem.find('.//xs:restriction', ns)
                if simple_type is not None:
                    base_type = simple_type.get('base')
                    if base_type:
                        col_type = base_type
            return col_type

        # Parse complexTypes
        complex_types = {}
        for ctype in root.findall('xs:complexType', ns):
            name = ctype.get('name')
            if not name:
                continue
            fields = []
            
            # Look for attributes first (these are important for XML structure)
            for attr in ctype.findall('.//xs:attribute', ns):
                attr_name = attr.get('name')
                attr_type = attr.get('type', 'xs:string')
                if attr_name:
                    fields.append((attr_name, attr_type))
            
            # Look for sequence elements
            sequence = ctype.find('.//xs:sequence', ns)
            if sequence is not None:
                for elem in sequence.findall('xs:element', ns):
                    col_name = elem.get('name')
                    col_type = _get_element_type(elem, ns)
                    if col_name:
                        fields.append((col_name, col_type))
            
            # Look for direct elements (choice/all)
            for elem in ctype.findall('.//xs:element', ns):
                col_name = elem.get('name')
                col_type = _get_element_type(elem, ns)
                if col_name and (col_name, col_type) not in fields:
                    fields.append((col_name, col_type))
            
            complex_types[name] = fields
        
        # Parse top-level elements
        elements = {}
        for elem in root.findall('xs:element', ns):
            table_name = elem.get('name')
            type_name = elem.get('type')
            if table_name and type_name:
                # Remove namespace prefix if present
                if ':' in type_name:
                    type_name = type_name.split(':')[-1]
                if type_name in complex_types:
                    elements[table_name] = complex_types[type_name]
                else:
                    # Simple element
                    elements[table_name] = [(table_name, type_name)]
        
        # If no top-level elements found, use complex types as elements
        # This is common in PDBML schemas where each complex type represents a table
        if not elements:
            for type_name, fields in complex_types.items():
                # Convert type names to element names (remove 'Type' suffix)
                elem_name = type_name
                if elem_name.endswith('Type'):
                    elem_name = elem_name[:-4]
                # Convert camelCase to snake_case for mmCIF compatibility
                import re
                elem_name = re.sub(r'([A-Z])', r'_\1', elem_name).lower().strip('_')
                elements[elem_name] = fields
        
        result = {
            'elements': elements,
            'attributes': {},  # Could be extended to parse attributes
            'required_elements': {},  # Could be extended to parse required elements
            'default_values': {},  # Could be extended to parse default values
            'complex_types': complex_types
        }
        # Store in unified cache
        self.cache_manager.set('xsd', cache_key, result)
        if not self.quiet:
            print(f"📋 Parsed {len(elements)} elements, {len(complex_types)} complex types")
        return result

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
        
        # Generate cache key based on both source files and their modification times
        cache_key_parts = []
        if self.dict_parser.source and Path(self.dict_parser.source).exists():
            dict_path = str(Path(self.dict_parser.source).resolve())
            dict_mtime = os.path.getmtime(self.dict_parser.source)
            cache_key_parts.append(f"dict_{dict_path}_{dict_mtime}")
        if self.xsd_parser.source and Path(self.xsd_parser.source).exists():
            xsd_path = str(Path(self.xsd_parser.source).resolve())
            xsd_mtime = os.path.getmtime(self.xsd_parser.source)
            cache_key_parts.append(f"xsd_{xsd_path}_{xsd_mtime}")
        
        cache_key = f"mapping_{hashlib.md5('|'.join(cache_key_parts).encode()).hexdigest()}"
        
        # Check unified cache
        cached = self.cache_manager.get('mapping_rules', cache_key)
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
        # Store in unified cache
        self.cache_manager.set('mapping_rules', cache_key, self._mapping_rules)
        return self._mapping_rules

    def _generate_mapping(
        self, 
        dict_meta: Dict[str, Any], 
        xsd_meta: Dict[str, Any]
    ) -> Dict[str, Any]:
        # Merge XSD structure with dictionary semantics
        category_mapping = {}
        item_mapping = {}
        
        # Use dictionary categories as the primary source
        for cat_name, cat_data in dict_meta['categories'].items():
            # Find matching XSD type
            xsd_type_name = f"{cat_name}Type"
            xsd_fields = xsd_meta['complex_types'].get(xsd_type_name, [])
            
            # Get all items for this category from dictionary
            cat_items = []
            for item_name, item_data in dict_meta['items'].items():
                if item_name.startswith(f"_{cat_name}."):
                    field_name = item_name[len(f"_{cat_name}."):]
                    cat_items.append(field_name)
            
            # Combine XSD fields and dictionary items - prioritize XSD fields
            all_fields = set()
            # First add all XSD fields (these are authoritative for XML structure)
            for field_name, field_type in xsd_fields:
                all_fields.add(field_name)
            # Then add dictionary items
            for field_name in cat_items:
                all_fields.add(field_name)
            
            category_mapping[cat_name] = {
                "fields": sorted(list(all_fields))
            }
            
            # Map individual items
            item_mapping[cat_name] = {}
            for field_name in all_fields:
                item_name = f"_{cat_name}.{field_name}"
                item_data = dict_meta['items'].get(item_name, {})
                
                # Get type from XSD or dictionary
                field_type = next(
                    (ft for fn, ft in xsd_fields if fn == field_name), 
                    item_data.get("item_type.code", "xs:string")
                )
                
                item_mapping[cat_name][field_name] = {
                    "type": field_type,
                    "enum": dict_meta['enumerations'].get(item_name),
                    "description": item_data.get("item_description.description", "")
                }
        
        # Also add any XSD-only categories
        for xsd_type_name, xsd_fields in xsd_meta['complex_types'].items():
            if xsd_type_name.endswith('Type'):
                cat_name = xsd_type_name[:-4]  # Remove 'Type' suffix
                
                # Convert camelCase to snake_case if needed
                import re
                cat_name_snake = re.sub(r'([A-Z])', r'_\1', cat_name).lower().strip('_')
                
                # Check both original and snake_case versions
                for possible_name in [cat_name, cat_name_snake]:
                    if possible_name not in category_mapping and possible_name not in dict_meta['categories']:
                        category_mapping[possible_name] = {
                            "fields": [fn for fn, ft in xsd_fields]
                        }
                        item_mapping[possible_name] = {}
                        for field_name, field_type in xsd_fields:
                            item_mapping[possible_name][field_name] = {
                                "type": field_type,
                                "enum": None,
                                "description": ""
                            }
                        break
        
        # Foreign keys from dictionary relationships
        fk_map = {}
        for rel in dict_meta['relationships']:
            # Handle different relationship types
            child_name = rel.get("item_linked.child_name") or rel.get("child_name")
            parent_name = rel.get("item_linked.parent_name") or rel.get("parent_name")
            child_cat = rel.get("child_category")
            parent_cat = rel.get("parent_category")
            
            if child_name and parent_name:
                if child_cat and parent_cat:
                    # New format with explicit categories
                    child_field = child_name.strip("_").split(".")[-1] if "." in child_name else child_name
                    parent_field = parent_name.strip("_").split(".")[-1] if "." in parent_name else parent_name
                    fk_map[(child_cat, child_field)] = (parent_cat, parent_field)
                else:
                    # Legacy format - extract from field names
                    child_parts = child_name.strip("_").split(".")
                    parent_parts = parent_name.strip("_").split(".")
                    
                    if len(child_parts) == 2 and len(parent_parts) == 2:
                        fk_map[(child_parts[0], child_parts[1])] = (parent_parts[0], parent_parts[1])
        
        return {
            "category_mapping": category_mapping,
            "item_mapping": item_mapping,
            "fk_map": fk_map,
            "primary_keys": dict_meta.get("primary_keys", {})
        }

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
        self.namespace = PDBMLNamespace.get_default_namespace()
        self._category_keys_cache = {}
        # Get schema info once at init
        self.xsd_meta = self.mapping_generator.xsd_parser.parse(
            self.mapping_generator.xsd_parser.source
        )
        # Cache mapping rules to avoid repeated expensive calls
        self._mapping_rules = None
        # Cache XSD tree to avoid repeated parsing
        self._xsd_tree = None
        # Pre-compute attribute/element mappings to avoid expensive XPath searches
        self._attribute_fields_cache = {}

    def _precompute_attribute_fields(self):
        """Pre-compute which fields are attributes for all categories to avoid repeated XPath queries"""
        if self._attribute_fields_cache:
            return  # Already computed
            
        tree = self._get_xsd_tree()
        if tree is None:
            return
            
        ns = {'xs': 'http://www.w3.org/2001/XMLSchema'}
        root = tree.getroot()
        
        # Pre-scan all complex types to build attribute/element mappings
        for type_elem in root.findall(".//xs:complexType", ns):
            type_name = type_elem.get('name')
            if not type_name or not type_name.endswith('Type'):
                continue
                
            cat_name = type_name[:-4]  # Remove 'Type' suffix
            if cat_name not in self._attribute_fields_cache:
                self._attribute_fields_cache[cat_name] = {'attributes': set(), 'elements': set()}
            
            # Collect attributes
            for attr_elem in type_elem.findall(".//xs:attribute", ns):
                attr_name = attr_elem.get('name')
                if attr_name:
                    self._attribute_fields_cache[cat_name]['attributes'].add(attr_name)
            
            # Collect elements  
            for elem_elem in type_elem.findall(".//xs:element", ns):
                elem_name = elem_elem.get('name')
                if elem_name:
                    self._attribute_fields_cache[cat_name]['elements'].add(elem_name)

    def _get_xsd_tree(self):
        """Get cached XSD tree to avoid repeated parsing"""
        if self._xsd_tree is not None:
            return self._xsd_tree
            
        xsd_path = self.mapping_generator.xsd_parser.source
        if not xsd_path or not Path(xsd_path).exists():
            return None
            
        # Generate cache key
        xsd_path_resolved = str(Path(xsd_path).resolve())
        mtime = os.path.getmtime(xsd_path)
        cache_key = f"xsd_tree_{hashlib.md5(f'{xsd_path_resolved}_{mtime}'.encode()).hexdigest()}"
        
        # Check unified cache first
        cached_tree = self.mapping_generator.cache_manager.get('xsd_trees', cache_key)
        if cached_tree:
            self._xsd_tree = cached_tree
            return self._xsd_tree
        
        # Parse and cache the tree
        try:
            self._xsd_tree = ElementTree.parse(xsd_path)
            self.mapping_generator.cache_manager.set('xsd_trees', cache_key, self._xsd_tree)
        except Exception:
            self._xsd_tree = None
            
        return self._xsd_tree

    @property
    def mapping_rules(self) -> Dict[str, Any]:
        """Cached access to mapping rules"""
        if self._mapping_rules is None:
            self._mapping_rules = self.mapping_generator.get_mapping_rules()
        return self._mapping_rules

    def _clean_value(self, value: Any) -> str:
        """Clean and normalize values from mmCIF data."""
        if value is None:
            return ""
        
        str_value = str(value)
        
        # Remove surrounding quotes if they exist
        if len(str_value) >= 2:
            if (str_value.startswith('"') and str_value.endswith('"')) or \
               (str_value.startswith("'") and str_value.endswith("'")):
                str_value = str_value[1:-1]
        
        return str_value

    @lru_cache(maxsize=256)
    def _get_field_type(self, cat_name: str, field_name: str) -> str:
        """Get the XSD type for a field"""
        type_name = f"{cat_name}Type"
        if type_name in self.xsd_meta['complex_types']:
            for fn, ft in self.xsd_meta['complex_types'][type_name]:
                if fn == field_name:
                    return ft
        return "xs:string"  # Default to string if type not found

    def _is_typed_field(self, field_type: str) -> bool:
        """Check if a field has a specific non-string type that requires proper formatting"""
        typed_fields = ['xsd:integer', 'xsd:int', 'xsd:decimal', 'xsd:double', 'xsd:float', 'xsd:boolean', 'xsd:date', 'xsd:dateTime']
        return field_type in typed_fields

    @lru_cache(maxsize=256)
    def _is_attribute_field(self, cat_name: str, field_name: str) -> bool:
        """Determine if a field should be an XML attribute based on schema and conventions"""
        # Ensure attribute fields are pre-computed
        self._precompute_attribute_fields()
        
        # Get mapping info
        mapping = self.mapping_rules
        
        # Check if field is a primary key - primary keys are typically attributes
        primary_keys = mapping.get("primary_keys", {})
        if cat_name in primary_keys:
            pk = primary_keys[cat_name]
            if isinstance(pk, list):
                if field_name in pk:
                    return True
            elif field_name == pk:
                return True
        
        # Check pre-computed attribute/element mappings
        if cat_name in self._attribute_fields_cache:
            cache_entry = self._attribute_fields_cache[cat_name]
            # If explicitly defined as attribute in schema
            if field_name in cache_entry['attributes']:
                return True
            # If explicitly defined as element in schema
            if field_name in cache_entry['elements']:
                return False
        
        # Fallback to pattern matching for fields not explicitly defined in schema
        attr_patterns = ['id', 'name', 'type', 'value', 'code']
        return (field_name in attr_patterns or 
               field_name.endswith('_id') or 
               field_name.endswith('_no') or 
               field_name.endswith('_index'))

    def convert_to_pdbml(self, mmcif_container: MMCIFDataContainer) -> str:
        """Convert mmCIF container to PDBML XML string"""
        mapping = self.mapping_rules
        
        # Assume single data block for simplicity
        block = next(iter(mmcif_container))
        
        # Use ElementTree for XML generation (more similar to legacy approach)
        root = ET.Element('datablock')
        root.set('xmlns', 'http://pdbml.pdb.org/schema/pdbx-v50.xsd')
        root.set('{http://www.w3.org/2001/XMLSchema-instance}schemaLocation', 
                'http://pdbml.pdb.org/schema/pdbx-v50.xsd pdbx-v50.xsd')
        root.set('datablockName', block.name)
        
        for cat_name in block.data:
            # cat_name is already a string (category name), remove leading underscore
            cat_name_clean = cat_name.lstrip("_")
            
            # Check if we have mapping for this category
            if cat_name_clean not in mapping["category_mapping"]:
                if not self.quiet:
                    print(f"Warning: No mapping found for category {cat_name_clean}")
                continue
            
            # Get the actual category object
            category = block.data[cat_name]
            if category.row_count == 0:
                continue
            
            # Generate category element
            category_elem = ET.SubElement(root, f'{cat_name_clean}Category')
            
            # Process each row in the category
            for i in range(category.row_count):
                row = category[i]
                
                # Get mapped fields for this category
                mapped_fields = mapping["category_mapping"][cat_name_clean]["fields"]
                
                # Create row element
                row_elem = ET.SubElement(category_elem, cat_name_clean)
                
                # Determine which fields should be attributes vs elements based on schema
                for field in mapped_fields:
                    # Check if field exists in row data
                    value = row.data.get(field)
                    if value is not None:
                        clean_value = self._clean_value(value)
                        
                        if self._is_attribute_field(cat_name_clean, field):
                            # Add as attribute
                            if clean_value and str(clean_value) not in ['', '.', '?']:
                                row_elem.set(field, clean_value)
                        else:
                            # Add as element
                            if str(clean_value) in ['', '.', '?']:
                                # For missing values, handle based on field type
                                field_type = self._get_field_type(cat_name_clean, field)
                                if self._is_typed_field(field_type):
                                    # For typed fields (integer, decimal, etc.), provide appropriate default
                                    field_elem = ET.SubElement(row_elem, field)
                                    if 'integer' in field_type.lower() or 'int' in field_type.lower():
                                        # For integer fields, use 1 as default (common for sequence IDs)
                                        field_elem.text = "1"
                                    elif 'decimal' in field_type.lower() or 'double' in field_type.lower() or 'float' in field_type.lower():
                                        # For numeric fields, use 0.0 as default
                                        field_elem.text = "0.0"
                                    else:
                                        # For other typed fields, use empty string
                                        field_elem.text = ""
                                else:
                                    # For string fields, use empty elements
                                    field_elem = ET.SubElement(row_elem, field)
                                    field_elem.text = ""
                            else:
                                # Normal element with value
                                field_elem = ET.SubElement(row_elem, field)
                                field_elem.text = clean_value
        
        # Convert to string using ElementTree (similar to legacy approach)
        xml_string = ET.tostring(root, encoding='utf-8').decode('utf-8')
        return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_string

# ====================== Relationship Resolver ======================
class RelationshipResolver:
    """Resolves entity relationships for nested JSON output"""
    def __init__(self, mapping_generator: MappingGenerator):
        self.mapping_generator = mapping_generator
        self._mapping_rules = None
        
    @property
    def mapping_rules(self) -> Dict[str, Any]:
        """Cached access to mapping rules"""
        if self._mapping_rules is None:
            self._mapping_rules = self.mapping_generator.get_mapping_rules()
        return self._mapping_rules

    def resolve_relationships(self, xml_content: str) -> Dict[str, Any]:
        # Parse XML to flat dict
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
                # Create row data from both attributes and child elements
                row_data = {}
                # Add attributes
                row_data.update(item_elem.attrib)
                # Add child elements
                for child in item_elem:
                    child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
                    row_data[child_tag] = child.text
                
                flat.setdefault(entity_name, []).append(row_data)
        
        # Use FK map to nest and convert lists to dictionaries using primary keys
        mapping = self.mapping_rules
        fk_map = mapping["fk_map"]
        primary_keys = mapping.get("primary_keys", {})
        
        # Filter FK map to only include ownership relationships (not references)
        ownership_fk_map = self._filter_ownership_relationships(fk_map, flat)
        
        # Identify categories that are ONLY children (never parents) and have duplicate primary keys
        child_only_cats = set()
        parent_cats = {p for (c, _), (p, _) in ownership_fk_map.items()}
        child_cats = {c for (c, _) in ownership_fk_map.keys()}
        
        for cat in child_cats:
            if cat not in parent_cats:  # Category is only a child, never a parent
                # Check if it has duplicate primary key values (indicating it should be processed as arrays)
                pk_field = primary_keys.get(cat, 'id')
                pk_values = [row.get(pk_field) for row in flat.get(cat, [])]
                if len(pk_values) != len(set(pk_values)):  # Duplicate primary keys found
                    child_only_cats.add(cat)
        
        # Convert lists to dictionaries using primary keys (except for child-only categories)
        indexed = {}
        for entity_name, entity_list in flat.items():
            if entity_name in child_only_cats:
                # For child-only categories with duplicate keys, keep as list and use index as key
                entity_dict = {}
                for i, row in enumerate(entity_list):
                    entity_dict[str(i)] = row
                indexed[entity_name] = entity_dict
            else:
                # Normal indexing by primary key
                pk_field = primary_keys.get(entity_name, 'id')  # Default to 'id' if not specified
                entity_dict = {}
                for row in entity_list:
                    pk_value = row.get(pk_field)
                    if pk_value is not None:
                        entity_dict[str(pk_value)] = row
                    else:
                        # If no primary key value, use index as fallback
                        entity_dict[str(len(entity_dict))] = row
                indexed[entity_name] = entity_dict
        
        # Build parent lookup using indexed structure
        parent_lookup = {}
        for (child_cat, child_col), (parent_cat, parent_col) in ownership_fk_map.items():
            parent_lookup.setdefault(parent_cat, {})
            for pk, row in indexed.get(parent_cat, {}).items():
                parent_lookup[parent_cat][pk] = row
        
        # Assign children using indexed structure
        for (child_cat, child_col), (parent_cat, parent_col) in ownership_fk_map.items():
            for child_pk, row in indexed.get(child_cat, {}).items():
                fk = row.get(child_col)
                if fk and str(fk) in indexed.get(parent_cat, {}):
                    parent = indexed[parent_cat][str(fk)]
                    if child_cat not in parent:
                        parent[child_cat] = []
                    # Handle multiple children as array - more intuitive than object with ID keys
                    parent[child_cat].append(row)
        
        # Return top-level categories
        # A category is top-level if it's not actually nested as a child in the current data
        
        # Find which categories are actually nested as children in the current data
        actually_nested_cats = set()
        for entity_name, entity_dict in indexed.items():
            for pk, entity_data in entity_dict.items():
                # Check if this entity has any nested children (categories as keys)
                for key in entity_data.keys():
                    if key in indexed and isinstance(entity_data.get(key), list):
                        # This key represents a nested category that's actually populated
                        actually_nested_cats.add(key)
        
        top = {}
        for k, v in indexed.items():
            # Only include if it's NOT actually nested as a child in the current data
            # This prevents duplication where nested children also appear at top level
            if k not in actually_nested_cats:
                # Convert top-level category dictionaries to arrays for consistency
                # This makes all collections uniform (arrays instead of objects with ID keys)
                if isinstance(v, dict) and len(v) > 0:
                    # Convert dictionary to array, preserving order by sorting keys
                    top[k] = [item for key, item in sorted(v.items())]
                else:
                    top[k] = v
        return top

    def _filter_ownership_relationships(self, fk_map: Dict, data: Dict) -> Dict:
        """
        Filter FK map to include only ownership relationships (not references).
        Uses data-driven analysis based on dictionary metadata and relationship cardinality.
        """
        # Get dictionary metadata for relationship analysis
        dict_meta = self.mapping_generator.dict_parser.parse(
            self.mapping_generator.dict_parser.source
        )
        
        ownership_fk_map = {}
        
        for (child_cat, child_field), (parent_cat, parent_field) in fk_map.items():
            # Analyze if this is an ownership relationship or a reference relationship
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
        """
        Determine if a relationship represents ownership (parent owns child) 
        rather than reference (child references parent).
        
        Uses data-driven analysis from dictionary metadata and relationship patterns.
        """
        # 1. Check dictionary relationship metadata for explicit ownership indicators
        for rel in dict_meta.get('relationships', []):
            rel_child_name = rel.get('child_name', '').strip('_')
            rel_parent_name = rel.get('parent_name', '').strip('_')
            
            # Match this relationship in the dictionary
            if (rel_child_name.endswith(f'{child_cat}.{child_field}') and 
                rel_parent_name.endswith(f'{parent_cat}.{parent_field}')):
                
                # Check for explicit ownership indicators in the relationship metadata
                if self._has_ownership_indicators(rel):
                    return True
                if self._has_reference_indicators(rel):
                    return False
        
        # 2. Analyze cardinality from actual data
        cardinality_score = self._analyze_cardinality(
            child_cat, child_field, parent_cat, parent_field, data
        )
        
        # 3. Analyze semantic naming patterns from dictionary item definitions
        semantic_score = self._analyze_semantic_patterns(
            child_cat, child_field, parent_cat, dict_meta
        )
        
        # 4. Analyze category hierarchy from dictionary metadata
        hierarchy_score = self._analyze_category_hierarchy(
            child_cat, parent_cat, dict_meta
        )
        
        # Combine scores to determine ownership vs reference
        total_score = cardinality_score + semantic_score + hierarchy_score
        
        # Threshold for ownership (positive scores indicate ownership)
        return total_score > 0
    
    def _has_ownership_indicators(self, rel: Dict) -> bool:
        """Check for explicit ownership indicators in relationship metadata"""
        # Look for ownership-related terms in relationship descriptions
        description = rel.get('description', '').lower()
        ownership_terms = ['belongs to', 'owned by', 'part of', 'contained in', 'member of']
        return any(term in description for term in ownership_terms)
    
    def _has_reference_indicators(self, rel: Dict) -> bool:
        """Check for explicit reference indicators in relationship metadata"""
        # Look for reference-related terms in relationship descriptions
        description = rel.get('description', '').lower()
        reference_terms = ['refers to', 'references', 'lookup', 'type of', 'code for']
        return any(term in description for term in reference_terms)
    
    def _analyze_cardinality(
        self, 
        child_cat: str, 
        child_field: str, 
        parent_cat: str, 
        parent_field: str, 
        data: Dict
    ) -> float:
        """
        Analyze relationship cardinality from actual data.
        
        Key insight: We need to look at the DIRECTION of the relationship:
        - True ownership: Parent category naturally contains/owns children 
          (e.g., struct_asym owns atom_site records within that asymmetric unit)
        - Reference: Child references a lookup/type table 
          (e.g., atom_site.type_symbol references atom_type.symbol for type info)
        
        For references to lookup/type tables, even if many children reference 
        the same parent, this indicates REFERENCE, not ownership.
        """
        if child_cat not in data or parent_cat not in data:
            return 0.0
        
        child_data = data[child_cat]
        parent_data = data[parent_cat]
        
        if not child_data or not parent_data:
            return 0.0
        
        # First, detect if parent category is a lookup/reference table
        # These patterns strongly indicate reference relationships
        lookup_table_patterns = [
            'type', 'class', 'method', 'status', 'code', 'symbol',
            'enum', 'dict', 'list', 'table', 'ref'
        ]
        
        is_lookup_table = any(pattern in parent_cat.lower() for pattern in lookup_table_patterns)
        is_reference_field = any(pattern in child_field.lower() for pattern in lookup_table_patterns)
        
        # Special handling for known reference patterns
        if is_lookup_table or is_reference_field:
            # This is very likely a reference relationship, not ownership
            return -30.0
        
        # For non-lookup relationships, analyze cardinality patterns
        parent_to_children = {}
        for child_row in child_data:
            fk_value = child_row.get(child_field)
            if fk_value:
                parent_to_children.setdefault(fk_value, 0)
                parent_to_children[fk_value] += 1
        
        if not parent_to_children:
            return 0.0
        
        # Check if this looks like a natural hierarchy vs reference
        # For ownership, we expect:
        # 1. Most or all parents have children
        # 2. Reasonable distribution of children per parent
        parent_count = len(parent_data)
        referenced_parent_count = len(parent_to_children)
        coverage_ratio = referenced_parent_count / parent_count if parent_count > 0 else 0
        
        # If only a small fraction of parents are referenced, likely reference table
        if coverage_ratio < 0.3:
            return -20.0
        
        # Calculate average children per parent
        avg_children = sum(parent_to_children.values()) / len(parent_to_children)
        
        # For true ownership relationships, we expect moderate child counts
        # Very high child counts per parent often indicate reference relationships
        if avg_children > 10:  # Many children per parent = likely reference
            return -15.0
        elif avg_children > 5:
            return -5.0
        elif avg_children > 2:
            return 10.0
        else:
            return 20.0  # 1:1 or 1:2 relationships often indicate ownership
    
    def _analyze_semantic_patterns(
        self, 
        child_cat: str, 
        child_field: str, 
        parent_cat: str, 
        dict_meta: Dict
    ) -> float:
        """
        Analyze semantic naming patterns from dictionary item definitions.
        """
        # Get item definition for the child field
        child_item_name = f'_{child_cat}.{child_field}'
        child_item = dict_meta.get('items', {}).get(child_item_name, {})
        
        description = child_item.get('item.description', '').lower()
        name = child_item.get('item.name', '').lower()
        
        # Analyze field naming patterns
        semantic_score = 0.0
        
        # Strong reference indicators in field names
        strong_reference_patterns = [
            'type_symbol', 'symbol', 'type', 'code', 'class', 'method', 
            'status', 'enum', 'category', 'kind'
        ]
        
        if any(pattern in child_field.lower() for pattern in strong_reference_patterns):
            semantic_score -= 40  # Strong reference indicator
        
        # Strong ownership indicators
        ownership_patterns = [
            f'{parent_cat}_id', 'asym_id', 'entity_id', 'struct_id'
        ]
        
        if any(pattern in child_field.lower() for pattern in ownership_patterns):
            semantic_score += 30  # Strong ownership indicator
        
        # Primary key references often indicate ownership
        if child_field == 'id' or child_field.endswith('_id'):
            # Check if this ID field references the parent's primary key
            if parent_cat in child_field or child_field == f'{parent_cat}_id':
                semantic_score += 30  # Strong ownership indicator
        
        # Category name inclusion patterns
        if parent_cat in child_cat:
            # Child category name contains parent name (e.g., atom_site -> struct_asym)
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
        """
        Analyze category hierarchy patterns from dictionary metadata.
        """
        # Get category definitions
        child_category = dict_meta.get('categories', {}).get(child_cat, {})
        parent_category = dict_meta.get('categories', {}).get(parent_cat, {})
        
        hierarchy_score = 0.0
        
        # Check category descriptions for hierarchy indicators
        child_desc = child_category.get('category.description', '').lower()
        parent_desc = parent_category.get('category.description', '').lower()
        
        # Analyze naming patterns
        if child_cat.startswith(parent_cat):
            # Child category name starts with parent name
            hierarchy_score += 25
        
        # Look for hierarchical terms in descriptions
        if child_desc and parent_desc:
            if 'detail' in child_desc or 'specific' in child_desc:
                hierarchy_score += 15
            if 'general' in parent_desc or 'summary' in parent_desc:
                hierarchy_score += 10
        
        # Special patterns for structural relationships
        structural_patterns = [
            (child_cat.endswith('_site'), parent_cat.endswith('_asym')),  # Sites belong to asymmetric units
            (child_cat.endswith('_atom'), parent_cat.endswith('_residue')),  # Atoms belong to residues
            ('author' in child_cat, 'label' in parent_cat),  # Author fields reference label fields
        ]
        
        for child_pattern, parent_pattern in structural_patterns:
            if child_pattern and parent_pattern:
                hierarchy_score += 20
                break
        
        return hierarchy_score

# ====================== Main Pipeline ======================
class MMCIFToPDBMLPipeline:
    """Orchestrates the complete conversion pipeline"""
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = "default",  # Use "default" as sentinel to allow None
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        # Set default schema paths if not provided
        if dict_path is None:
            dict_path = Path(__file__).parent / "schemas" / "mmcif_pdbx_v50.dic"
        if xsd_path == "default":
            xsd_path = Path(__file__).parent / "schemas" / "pdbx-v50.xsd"
            
        # Set up caching
        cache_manager = get_cache_manager(cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache"))
        
        # Set up metadata parsers
        dict_parser = DictionaryParser(cache_manager, quiet)
        xsd_parser = XSDParser(cache_manager, quiet)
        dict_parser.source = dict_path
        xsd_parser.source = xsd_path
        
        # Set up mapping generator
        mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache_manager, quiet)
        
        # Set up converter and resolver
        self.converter = PDBMLConverter(mapping_generator, permissive, quiet)
        self.resolver = RelationshipResolver(mapping_generator)
        self.validator = XMLSchemaValidator(xsd_path) if xsd_path and not permissive else None
    
    def process_mmcif_file(self, mmcif_path: Union[str, Path]) -> Dict[str, Any]:
        # Parse mmCIF
        parser = MMCIFParser()
        mmcif_container = parser.parse_file(mmcif_path)
        
        # Convert to PDBML
        pdbml_xml = self.converter.convert_to_pdbml(mmcif_container)
        
        # Validate
        # Validate
        if self.validator:
            try:
                validation = self.validator.validate(pdbml_xml)
            except ValidationError as e:
                # Convert ValidationError exception to the expected dictionary format
                validation = {
                    "valid": False,
                    "errors": str(e).split(';')
                }
        else:
            validation = {"valid": True, "errors": []}
        
        # Resolve relationships
        nested_json = self.resolver.resolve_relationships(pdbml_xml)
        
        return {
            "mmcif_data": mmcif_container,
            "pdbml_xml": pdbml_xml,
            "validation": validation,
            "nested_json": nested_json
        }