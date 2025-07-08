"""
PDBML Converter - Convert mmCIF to PDBX/PDBML XML format

Refactored to:
- Separate caching concerns into dedicated classes
- Split XML mapping into distinct components
- Use polymorphism for different parsers
- Reduce redundancy through abstraction
"""

import os
import re
import json
import hashlib
import threading
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from xml.etree import ElementTree as ET
from functools import lru_cache, wraps
from abc import ABC, abstractmethod

from .models import MMCIFDataContainer, DataBlock, Category
from .parser import MMCIFParser
from .validators import XMLSchemaValidator
from .schemas import (
    XMLLocation, XMLElementType, XMLGroupingType, XMLContainerType,
    PDBMLElement, PDBMLAttribute, DebugFile, get_numeric_fields, 
    is_null_value, PDBMLNamespace
)

# ====================== Caching System ======================
class CacheManager(ABC):
    """Abstract base class for cache management"""
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        pass

class MemoryCache(CacheManager):
    """In-memory cache with thread safety"""
    def __init__(self):
        self._cache = {}
        self._lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self._cache.get(key)
    
    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._cache[key] = value

class DiskCache(CacheManager):
    """Disk-based cache with file-based storage"""
    def __init__(self, cache_dir: str):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get(self, key: str) -> Optional[Any]:
        cache_file = self.cache_dir / f"{key}.json"
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception:
            try:
                cache_file.unlink()
            except Exception:
                pass
            return None
    
    def set(self, key: str, value: Any) -> None:
        cache_file = self.cache_dir / f"{key}.json"
        try:
            with open(cache_file, 'w') as f:
                json.dump(value, f)
        except Exception:
            pass

class HybridCache(CacheManager):
    """Combined memory and disk cache"""
    def __init__(self, cache_dir: str):
        self.memory_cache = MemoryCache()
        self.disk_cache = DiskCache(cache_dir)
    
    def get(self, key: str) -> Optional[Any]:
        # First try memory cache
        value = self.memory_cache.get(key)
        if value is not None:
            return value
        
        # Fallback to disk cache
        value = self.disk_cache.get(key)
        if value is not None:
            self.memory_cache.set(key, value)
        return value
    
    def set(self, key: str, value: Any) -> None:
        self.memory_cache.set(key, value)
        self.disk_cache.set(key, value)

# ====================== Metadata Parsers ======================
class MetadataParser(ABC):
    """Base class for metadata parsers"""
    def __init__(self, cache: CacheManager, quiet: bool = False):
        self.cache = cache
        self.quiet = quiet

    @abstractmethod
    def parse(self, source: Union[str, Path]) -> Dict[str, Any]:
        pass

class DictionaryParser(MetadataParser):
    """Parses mmCIF dictionary files"""
    def __init__(self, cache: CacheManager, quiet: bool = False):
        super().__init__(cache, quiet)
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
        cache_key = f"dict_{Path(dict_path).name}"
        cached = self.cache.get(cache_key)
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
        
        # Also parse any tabular data from the main parser
        try:
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
        except Exception as e:
            if not self.quiet:
                print(f"Warning: Could not parse tabular data: {e}")
        
        result = {
            "categories": categories,
            "items": items,
            "relationships": relationships,
            "enumerations": enumerations,
            "item_types": item_types
        }
        self.cache.set(cache_key, result)
        if not self.quiet:
            print(f"📚 Parsed {len(categories)} categories, {len(items)} items")
        return result

class XSDParser(MetadataParser):
    """Parses XSD schema files"""
    def __init__(self, cache: CacheManager, quiet: bool = False):
        super().__init__(cache, quiet)
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
        cache_key = f"xsd_{Path(xsd_path).name}"
        cached = self.cache.get(cache_key)
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
        
        # Parse complexTypes
        complex_types = {}
        for ctype in root.findall('xs:complexType', ns):
            name = ctype.get('name')
            if not name:
                continue
            fields = []
            
            # Look for sequence elements
            sequence = ctype.find('.//xs:sequence', ns)
            if sequence is not None:
                for elem in sequence.findall('xs:element', ns):
                    col_name = elem.get('name')
                    col_type = elem.get('type', 'xs:string')
                    if col_name:
                        fields.append((col_name, col_type))
            
            # Look for direct elements (choice/all)
            for elem in ctype.findall('.//xs:element', ns):
                col_name = elem.get('name')
                col_type = elem.get('type', 'xs:string')
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
        self.cache.set(cache_key, result)
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
        cache: CacheManager,
        quiet: bool = False
    ):
        self.dict_parser = dict_parser
        self.xsd_parser = xsd_parser
        self.cache = cache
        self.quiet = quiet
        self._mapping_rules = None

    def get_mapping_rules(self) -> Dict[str, Any]:
        if self._mapping_rules is not None:
            return self._mapping_rules
        cache_key = "mapping_rules"
        cached = self.cache.get(cache_key)
        if cached:
            self._mapping_rules = cached
            return cached
        if not self.quiet:
            print("🧩 Generating mapping rules...")
        dict_meta = self.dict_parser.parse(self.dict_parser.source)
        xsd_meta = self.xsd_parser.parse(self.xsd_parser.source)
        self._mapping_rules = self._generate_mapping(dict_meta, xsd_meta)
        self.cache.set(cache_key, self._mapping_rules)
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
            
            # Combine XSD fields and dictionary items
            all_fields = set(cat_items)
            for field_name, field_type in xsd_fields:
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
            
            if child_name and parent_name:
                child_parts = child_name.strip("_").split(".")
                parent_parts = parent_name.strip("_").split(".")
                
                if len(child_parts) == 2 and len(parent_parts) == 2:
                    fk_map[(child_parts[0], child_parts[1])] = (parent_parts[0], parent_parts[1])
        
        return {
            "category_mapping": category_mapping,
            "item_mapping": item_mapping,
            "fk_map": fk_map
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

    def convert_to_pdbml(self, mmcif_container: MMCIFDataContainer) -> str:
        """Convert mmCIF container to PDBML XML string"""
        mapping = self.mapping_generator.get_mapping_rules()
        
        # Assume single data block for simplicity
        block = next(iter(mmcif_container))
        
        # Generate XML manually to avoid namespace prefix issues
        xml_lines = ['<datablock xmlns="http://pdbml.pdb.org/schema/pdbx-v50.xsd" datablockName="{}">'.format(block.name)]
        
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
            xml_lines.append(f'  <{cat_name_clean}Category>')
            
            # Process each row in the category
            for i in range(category.row_count):
                row = category[i]
                
                # Get mapped fields for this category
                mapped_fields = mapping["category_mapping"][cat_name_clean]["fields"]
                item_mapping = mapping["item_mapping"][cat_name_clean]
                
                # Determine which fields should be attributes vs elements
                # Simple heuristic: ID fields and keys are attributes, others are elements
                attribute_fields = set()
                element_fields = set()
                
                for field in mapped_fields:
                    # Check if field exists in row data
                    value = row.data.get(field)
                    if value is not None and str(value) not in ['', '.', '?']:
                        # ID fields and short identifiers as attributes
                        if (field.endswith('_id') or field == 'id' or 
                            field in ['symbol', 'code', 'type'] or
                            len(str(value)) < 10):
                            attribute_fields.add(field)
                        else:
                            element_fields.add(field)
                
                # Build row element
                row_attrs = []
                for field in sorted(attribute_fields):
                    value = row.data.get(field)
                    if value is not None and str(value) not in ['', '.', '?']:
                        # Escape XML attribute value
                        escaped_value = str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&apos;')
                        row_attrs.append(f'{field}="{escaped_value}"')
                
                # Create row element
                if element_fields:
                    # Has child elements
                    attr_str = ' ' + ' '.join(row_attrs) if row_attrs else ''
                    xml_lines.append(f'    <{cat_name_clean}{attr_str}>')
                    
                    # Add child elements
                    for field in sorted(element_fields):
                        value = row.data.get(field)
                        if value is not None and str(value) not in ['', '.', '?']:
                            # Escape XML text content
                            escaped_value = str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                            xml_lines.append(f'      <{field}>{escaped_value}</{field}>')
                    
                    xml_lines.append(f'    </{cat_name_clean}>')
                else:
                    # Self-closing or empty element
                    attr_str = ' ' + ' '.join(row_attrs) if row_attrs else ''
                    xml_lines.append(f'    <{cat_name_clean}{attr_str}/>')
            
            xml_lines.append(f'  </{cat_name_clean}Category>')
        
        xml_lines.append('</datablock>')
        return '\n'.join(xml_lines)

# ====================== Relationship Resolver ======================
class RelationshipResolver:
    """Resolves entity relationships for nested JSON output"""
    def __init__(self, mapping_generator: MappingGenerator):
        self.mapping_generator = mapping_generator

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
        
        # Use FK map to nest
        mapping = self.mapping_generator.get_mapping_rules()
        fk_map = mapping["fk_map"]
        # Build parent lookup
        parent_lookup = {}
        for (child_cat, child_col), (parent_cat, parent_col) in fk_map.items():
            parent_lookup.setdefault(parent_cat, {})
            for row in flat.get(parent_cat, []):
                pk = row.get(parent_col)
                if pk is not None:
                    parent_lookup[parent_cat][pk] = row
        # Assign children
        for (child_cat, child_col), (parent_cat, parent_col) in fk_map.items():
            for row in flat.get(child_cat, []):
                fk = row.get(child_col)
                if fk in parent_lookup.get(parent_cat, {}):
                    parent = parent_lookup[parent_cat][fk]
                    parent.setdefault(child_cat, []).append(row)
        # Return top-level tables only (those that are not children)
        child_cats = {c for (c, _) in fk_map.keys()}
        top = {k: v for k, v in flat.items() if k not in child_cats}
        return top

# ====================== Main Pipeline ======================
class MMCIFToPDBMLPipeline:
    """Orchestrates the complete conversion pipeline"""
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        # Set up caching
        cache = HybridCache(cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache"))
        
        # Set up metadata parsers
        dict_parser = DictionaryParser(cache, quiet)
        xsd_parser = XSDParser(cache, quiet)
        dict_parser.source = dict_path
        xsd_parser.source = xsd_path
        
        # Set up mapping generator
        mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache, quiet)
        
        # Set up converter and resolver
        self.converter = PDBMLConverter(mapping_generator, permissive, quiet)
        self.resolver = RelationshipResolver(mapping_generator)
        self.validator = XMLSchemaValidator(xsd_path) if xsd_path else None
    
    def process_mmcif_file(self, mmcif_path: Union[str, Path]) -> Dict[str, Any]:
        # Parse mmCIF
        parser = MMCIFParser()
        mmcif_container = parser.parse_file(mmcif_path)
        
        # Convert to PDBML
        pdbml_xml = self.converter.convert_to_pdbml(mmcif_container)
        
        # Validate
        validation = self.validator.validate(pdbml_xml) if self.validator else {"valid": True, "errors": []}
        
        # Resolve relationships
        nested_json = self.resolver.resolve_relationships(pdbml_xml)
        
        return {
            "mmcif_data": mmcif_container,
            "pdbml_xml": pdbml_xml,
            "validation": validation,
            "nested_json": nested_json
        }