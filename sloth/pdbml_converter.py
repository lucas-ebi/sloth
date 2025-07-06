"""
PDBML Converter - Convert mmCIF to PDBX/PDBML XML format

This module provides functionality to convert mmCIF data to PDBML XML format
that conforms to the pdbx-v50.xsd schema, and handles relationship resolution
for nested JSON output.
"""

import os
import re
import json
import hashlib
import threading
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from functools import lru_cache, wraps
from xml.etree import ElementTree as ET
from lxml import etree

from .models import MMCIFDataContainer, Category
from .parser import MMCIFParser
from .schemas import XMLSchemaValidator
from .pdbml_enums import (
    XMLLocation, EssentialKey, RequiredAttribute,
    get_element_only_items, get_atom_site_defaults, get_anisotropic_defaults,
    get_problematic_field_replacement, is_null_value, get_numeric_fields
)


# Global cache for dictionary parsing results - shared across instances
_DICTIONARY_CACHE = {}
_DICTIONARY_CACHE_LOCK = threading.Lock()

# Global cache for XSD schema parsing results
_XSD_CACHE = {}
_XSD_CACHE_LOCK = threading.Lock()

def disk_cache(cache_dir: Optional[str] = None):
    """Decorator for disk-based caching of expensive operations."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if cache_dir is None:
                return func(*args, **kwargs)
                
            # Create cache key from function name and arguments
            cache_key = f"{func.__name__}_{hashlib.md5(str(args + tuple(kwargs.items())).encode()).hexdigest()}"
            cache_file = Path(cache_dir) / f"{cache_key}.json"
            
            # Try to load from cache
            if cache_file.exists():
                try:
                    with open(cache_file, 'r') as f:
                        return json.load(f)
                except Exception:
                    pass  # Fall through to compute
            
            # Compute result and cache it
            result = func(*args, **kwargs)
            
            # Save to cache
            try:
                Path(cache_dir).mkdir(exist_ok=True)
                with open(cache_file, 'w') as f:
                    json.dump(result, f)
            except Exception:
                pass  # Don't fail if we can't cache
                
            return result
        return wrapper
    return decorator


class XMLMappingGenerator:
    """
    Embedded XML mapping generator that creates mapping rules on-the-fly
    without requiring external JSON files.
    """
    
    def __init__(self, dict_file: Optional[Union[str, Path]] = None, xsd_file: Optional[Union[str, Path]] = None, cache_dir: Optional[str] = None):
        self.dict_file = dict_file
        self.xsd_file = xsd_file
        self.cache_dir = cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
        
        # Dictionary data structures - lazy loaded
        self._categories = None
        self._items = None
        self._relationships = None
        self._enumerations = None
        self._item_types = None
        
        # XSD schema data structures - lazy loaded
        self._xsd_elements = None
        self._xsd_attributes = None
        self._xsd_required_elements = None
        self._xsd_default_values = None
        self._xsd_complex_types = None
        
        # Cache for generated mapping rules
        self._mapping_rules_cache = None
        
        # Thread locks for lazy loading
        self._dict_lock = threading.Lock()
        self._xsd_lock = threading.Lock()
        
    @property
    def categories(self) -> Dict[str, Any]:
        """Lazy-loaded categories property."""
        if self._categories is None:
            with self._dict_lock:
                if self._categories is None:  # Double-check pattern
                    self._ensure_dictionary_parsed()
        return self._categories or {}
        
    @property
    def items(self) -> Dict[str, Any]:
        """Lazy-loaded items property."""
        if self._items is None:
            with self._dict_lock:
                if self._items is None:
                    self._ensure_dictionary_parsed()
        return self._items or {}
        
    @property
    def relationships(self) -> List[Dict[str, Any]]:
        """Lazy-loaded relationships property."""
        if self._relationships is None:
            with self._dict_lock:
                if self._relationships is None:
                    self._ensure_dictionary_parsed()
        return self._relationships or []
        
    @property
    def enumerations(self) -> Dict[str, List[str]]:
        """Lazy-loaded enumerations property."""
        if self._enumerations is None:
            with self._dict_lock:
                if self._enumerations is None:
                    self._ensure_dictionary_parsed()
        return self._enumerations or {}
        
    @property
    def item_types(self) -> Dict[str, Any]:
        """Lazy-loaded item_types property."""
        if self._item_types is None:
            with self._dict_lock:
                if self._item_types is None:
                    self._ensure_dictionary_parsed()
        return self._item_types or {}
        
    @property
    def xsd_elements(self) -> Dict[str, Any]:
        """Lazy-loaded XSD elements property."""
        if self._xsd_elements is None:
            with self._xsd_lock:
                if self._xsd_elements is None:
                    self._ensure_xsd_parsed()
        return self._xsd_elements or {}
        
    @property
    def xsd_complex_types(self) -> Dict[str, Any]:
        """Lazy-loaded XSD complex types property."""
        if self._xsd_complex_types is None:
            with self._xsd_lock:
                if self._xsd_complex_types is None:
                    self._ensure_xsd_parsed()
        return self._xsd_complex_types or {}
        
    def _ensure_dictionary_parsed(self):
        """Ensure dictionary is parsed, using global cache if possible."""
        if not self.dict_file:
            self._categories = {}
            self._items = {}
            self._relationships = []
            self._enumerations = {}
            self._item_types = {}
            return
            
        dict_path = str(Path(self.dict_file).resolve())
        
        # Check global cache first
        with _DICTIONARY_CACHE_LOCK:
            if dict_path in _DICTIONARY_CACHE:
                cached_data = _DICTIONARY_CACHE[dict_path]
                self._categories = cached_data['categories']
                self._items = cached_data['items']
                self._relationships = cached_data['relationships']
                self._enumerations = cached_data['enumerations']
                self._item_types = cached_data['item_types']
                return
        
        # Parse dictionary
        self._categories = {}
        self._items = {}
        self._relationships = []
        self._enumerations = {}
        self._item_types = {}
        
        try:
            self._parse_dictionary_structure()
            
            # Cache the results globally
            with _DICTIONARY_CACHE_LOCK:
                _DICTIONARY_CACHE[dict_path] = {
                    'categories': self._categories,
                    'items': self._items,
                    'relationships': self._relationships,
                    'enumerations': self._enumerations,
                    'item_types': self._item_types
                }
                
        except Exception as e:
            print(f"⚠️ Warning: Error parsing dictionary: {e}")
            # Initialize empty structures
            self._categories = {}
            self._items = {}
            self._relationships = []
            self._enumerations = {}
            self._item_types = {}
        
    @disk_cache()
    def get_mapping_rules(self) -> Dict[str, Any]:
        """Get comprehensive mapping rules, generating them if not cached."""
        if self._mapping_rules_cache is not None:
            return self._mapping_rules_cache
            
        # Generate cache key for disk caching
        cache_key = self._get_cache_key()
        
        # Try to load from disk cache
        cached_rules = self._load_mapping_rules_from_cache(cache_key)
        if cached_rules:
            self._mapping_rules_cache = cached_rules
            return cached_rules
        
        # Generate mapping rules
        mapping_rules = {
            "structural_mapping": {},
            "category_mapping": {},
            "item_mapping": {},
            "element_requirements": {},
            "attribute_requirements": {},
            "default_values": {},
            "validation_rules": {}
        }
        
        try:
            # Parse dictionary structure if available (uses lazy loading)
            if self.dict_file and Path(self.dict_file).exists():
                pass  # Dictionary will be parsed on-demand via properties
                
            # Parse XSD schema if available (uses lazy loading)
            if self.xsd_file and Path(self.xsd_file).exists():
                self._ensure_xsd_parsed()
                
            # Generate comprehensive mappings
            mapping_rules = self._generate_comprehensive_mapping()
            
        except Exception as e:
            print(f"⚠️ Error: Cannot generate mapping rules without valid dictionary/schema: {e}")
            print("Please ensure that dictionary and/or XSD files are provided and valid.")
            raise RuntimeError(f"Failed to generate mapping rules: {e}")
            
        # Cache the results in memory and disk
        self._mapping_rules_cache = mapping_rules
        self._save_mapping_rules_to_cache(cache_key, mapping_rules)
        
        return mapping_rules
        
    def _get_cache_key(self) -> str:
        """Generate cache key based on file paths and modification times."""
        key_parts = []
        
        if self.dict_file and Path(self.dict_file).exists():
            dict_path = Path(self.dict_file)
            key_parts.append(f"dict:{dict_path.name}:{dict_path.stat().st_mtime}")
            
        if self.xsd_file and Path(self.xsd_file).exists():
            xsd_path = Path(self.xsd_file)
            key_parts.append(f"xsd:{xsd_path.name}:{xsd_path.stat().st_mtime}")
            
        cache_string = "|".join(key_parts)
        return hashlib.md5(cache_string.encode()).hexdigest()
        
    def _load_mapping_rules_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Load mapping rules from disk cache."""
        if not self.cache_dir:
            return None
            
        cache_file = Path(self.cache_dir) / f"mapping_rules_{cache_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    return json.load(f)
            except Exception:
                return None
        return None
        
    def _save_mapping_rules_to_cache(self, cache_key: str, rules: Dict[str, Any]):
        """Save mapping rules to disk cache."""
        if not self.cache_dir:
            return
            
        try:
            Path(self.cache_dir).mkdir(exist_ok=True)
            cache_file = Path(self.cache_dir) / f"mapping_rules_{cache_key}.json"
            with open(cache_file, 'w') as f:
                json.dump(rules, f, indent=2)
        except Exception as e:
            print(f"⚠️ Warning: Could not save mapping rules to cache: {e}")
            
    def _ensure_xsd_parsed(self):
        """Ensure XSD schema is parsed, using global cache if possible."""
        if not self.xsd_file:
            self._xsd_elements = {}
            self._xsd_attributes = {}
            self._xsd_required_elements = {}
            self._xsd_default_values = {}
            self._xsd_complex_types = {}
            return
            
        xsd_path = str(Path(self.xsd_file).resolve())
        
        # Check global cache first
        with _XSD_CACHE_LOCK:
            if xsd_path in _XSD_CACHE:
                cached_data = _XSD_CACHE[xsd_path]
                self._xsd_elements = cached_data['elements']
                self._xsd_attributes = cached_data['attributes']
                self._xsd_required_elements = cached_data['required_elements']
                self._xsd_default_values = cached_data['default_values']
                self._xsd_complex_types = cached_data['complex_types']
                return
        
        # Parse XSD schema
        self._xsd_elements = {}
        self._xsd_attributes = {}
        self._xsd_required_elements = {}
        self._xsd_default_values = {}
        self._xsd_complex_types = {}
        
        try:
            self._parse_xsd_schema()
            
            # Cache the results globally
            with _XSD_CACHE_LOCK:
                _XSD_CACHE[xsd_path] = {
                    'elements': self._xsd_elements,
                    'attributes': self._xsd_attributes,
                    'required_elements': self._xsd_required_elements,
                    'default_values': self._xsd_default_values,
                    'complex_types': self._xsd_complex_types
                }
                
        except Exception as e:
            print(f"⚠️ Warning: Error parsing XSD schema: {e}")
            # Initialize empty structures
            self._xsd_elements = {}
            self._xsd_attributes = {}
            self._xsd_required_elements = {}
            self._xsd_default_values = {}
            self._xsd_complex_types = {}
        
    def _parse_dictionary_structure(self):
        """Parse complete dictionary structure with all metadata"""
        if not self.dict_file:
            print("⚠️ No dictionary file provided")
            return
            
        current_save = None
        current_block = []
        in_save_frame = False
        save_count = 0
        category_count = 0
        
        try:
            with open(self.dict_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    
                    if not line or line.startswith('#'):
                        continue
                        
                    if line.startswith('save_'):
                        if in_save_frame and current_save:
                            self._process_save_frame(current_save, current_block)
                            save_count += 1
                            if '_category.id' in '\n'.join(current_block):
                                category_count += 1
                        current_save = line[5:]
                        current_block = []
                        in_save_frame = True
                        
                    elif line == 'save_':
                        if in_save_frame and current_save:
                            self._process_save_frame(current_save, current_block)
                            save_count += 1
                            if '_category.id' in '\n'.join(current_block):
                                category_count += 1
                        current_save = None
                        current_block = []
                        in_save_frame = False
                        
                    elif in_save_frame:
                        current_block.append(line)
                        
            # Debug: show some key categories
            debug_cats = ['entry', 'database_2', 'chem_comp_angle', 'atom_site']
            for cat_name in debug_cats:
                if cat_name not in self._categories:
                    print(f"⚠️ Warning: Category {cat_name}: not found in dictionary")
        except Exception as e:
            print(f"⚠️ Warning: Error parsing dictionary: {e}")
            traceback.print_exc()
                        
    def _process_save_frame(self, save_name: str, block: List[str]):
        """Process individual save frame to extract metadata"""
        block_text = '\n'.join(block)
        
        # Extract category definitions
        if '_category.id' in block_text:
            self._extract_category_info(save_name, block_text)
            
        # Extract item definitions  
        if '_item.name' in block_text:
            self._extract_item_info(save_name, block_text)
            
        # Extract item type definitions
        if '_item_type.code' in block_text:
            self._extract_item_type_info(save_name, block_text)
            
        # Extract enumeration definitions
        if '_item_enumeration.value' in block_text:
            self._extract_enumeration_info(save_name, block_text)
            
        # Extract relationship definitions
        if '_item_linked.parent_name' in block_text:
            self._extract_relationship_info(save_name, block_text)
            
    def _extract_category_info(self, save_name: str, block_text: str):
        """Extract category information including keys"""
        # Extract category ID
        cat_match = re.search(r'_category\.id\s+(\S+)', block_text)
        if not cat_match:
            return
            
        cat_id = cat_match.group(1).strip()
        
        # Initialize category data
        self._categories[cat_id] = {
            'id': cat_id,
            'save_name': save_name,
            'description': '',
            'mandatory': 'no',
            'keys': []
        }
        
        # Extract description
        desc_match = re.search(r'_category\.description\s*\n\s*;([^;]*);', block_text, re.DOTALL)
        if desc_match:
            self._categories[cat_id]['description'] = desc_match.group(1).strip()
            
        # Extract mandatory status
        mandatory_match = re.search(r'_category\.mandatory_code\s+(\S+)', block_text)
        if mandatory_match:
            self._categories[cat_id]['mandatory'] = mandatory_match.group(1).strip()
            
        # Extract category keys from both single line and loop structures
        if '_category_key.name' in block_text:
            # First, try to find single line keys
            single_key_pattern = r'_category_key\.name\s+[\'"]([^\'"]+)[\'"]'
            single_key_matches = re.findall(single_key_pattern, block_text)
            
            for key_item in single_key_matches:
                key_item = key_item.strip()
                if key_item.startswith('_' + cat_id + '.'):
                    item_name = key_item[len('_' + cat_id + '.'):]
                    self._categories[cat_id]['keys'].append(item_name)
            
            # Then, try to find loop-based keys - more robust approach
            if 'loop_' in block_text:
                # Find the loop block that contains _category_key.name
                loop_pattern = r'loop_\s*\n\s*_category_key\.name\s*\n((?:\s*[^\n#]+\n)*)'
                loop_match = re.search(loop_pattern, block_text)
                if loop_match:
                    key_lines = loop_match.group(1).strip().split('\n')
                    for line in key_lines:
                        line = line.strip()
                        if line and not line.startswith('_') and not line.startswith('#'):
                            # Remove quotes and extract item name
                            key_item = line.strip('\'"').strip()
                            if key_item.startswith('_' + cat_id + '.'):
                                item_name = key_item[len('_' + cat_id + '.'):]
                                if item_name not in self._categories[cat_id]['keys']:  # Avoid duplicates
                                    self._categories[cat_id]['keys'].append(item_name)
                else:
                    print(f"⚠️ Warning: Loop pattern did not match in {cat_id}")
        else:
            print(f"⚠️ Warning: No _category_key.name found in {cat_id}")
                                        
    def _extract_item_info(self, save_name: str, block_text: str):
        """Extract item information including data types and constraints"""
        # Extract item name
        item_match = re.search(r'_item\.name\s+[\'"]([^\'"]+)[\'"]', block_text)
        if not item_match:
            return
            
        item_name = item_match.group(1).strip()
        
        # Initialize item data
        self._items[item_name] = {
            'name': item_name,
            'save_name': save_name,
            'category_id': '',
            'description': '',
            'mandatory': 'no',
            'data_type': '',
            'constraints': []
        }
        
        # Extract category ID
        cat_match = re.search(r'_item\.category_id\s+(\S+)', block_text)
        if cat_match:
            self._items[item_name]['category_id'] = cat_match.group(1).strip()
            
        # Extract description
        desc_match = re.search(r'_item\.description\s*\n\s*;([^;]*);', block_text, re.DOTALL)
        if desc_match:
            self._items[item_name]['description'] = desc_match.group(1).strip()
            
        # Extract mandatory status
        mandatory_match = re.search(r'_item\.mandatory_code\s+(\S+)', block_text)
        if mandatory_match:
            self._items[item_name]['mandatory'] = mandatory_match.group(1).strip()
            
    def _extract_item_type_info(self, save_name: str, block_text: str):
        """Extract item type information for data validation"""
        # Extract type code
        type_match = re.search(r'_item_type\.code\s+[\'"]([^\'"]+)[\'"]', block_text)
        if not type_match:
            return
            
        type_code = type_match.group(1).strip()
        
        # Initialize type data
        self._item_types[type_code] = {
            'code': type_code,
            'save_name': save_name,
            'primitive_code': '',
            'construct': '',
            'detail': ''
        }
        
        # Extract primitive code
        prim_match = re.search(r'_item_type\.primitive_code\s+(\S+)', block_text)
        if prim_match:
            self._item_types[type_code]['primitive_code'] = prim_match.group(1).strip()
            
        # Extract construct
        construct_match = re.search(r'_item_type\.construct\s*\n\s*;([^;]*);', block_text, re.DOTALL)
        if construct_match:
            self._item_types[type_code]['construct'] = construct_match.group(1).strip()
            
    def _extract_enumeration_info(self, save_name: str, block_text: str):
        """Extract enumeration values for validation"""
        # The save_name already contains the item name (e.g., "_atom_site.group_PDB")
        # So we use that as the enumeration key
        
        if save_name not in self._enumerations:
            self._enumerations[save_name] = []
            
        # Extract enumeration values from loop
        if 'loop_' in block_text and '_item_enumeration.value' in block_text:
            # Look for the pattern where enumeration values are listed
            # Find the start of the loop values
            loop_start = block_text.find('_item_enumeration.value')
            if loop_start != -1:
                # Get the text after the loop header
                after_loop = block_text[loop_start:]
                lines = after_loop.split('\n')
                
                # Skip header lines and find actual values
                in_values = False
                for line in lines:
                    line = line.strip()
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                        
                    # If we see an underscore line, we're done with this loop
                    if line.startswith('_') and not line.startswith('_item_enumeration'):
                        break
                        
                    # If we see the next save block, we're done
                    if line.startswith('save_'):
                        break
                        
                    # If this is a header line, skip it
                    if line.startswith('_item_enumeration'):
                        in_values = True
                        continue
                        
                    # If we're in the values section and this doesn't start with underscore
                    if in_values and not line.startswith('_'):
                        # Remove quotes and whitespace
                        value = line.strip('\'"').strip()
                        if value and value not in self._enumerations[save_name]:
                            self._enumerations[save_name].append(value)
                            
    def _extract_relationship_info(self, save_name: str, block_text: str):
        """Extract parent-child relationships"""
        # Extract parent-child relationships from loops
        if 'loop_' in block_text and '_item_linked.parent_name' in block_text:
            # Parse loop structure
            parent_pattern = r'_item_linked\.parent_name\s*\n((?:\s*[^\n]+\n)*)'
            child_pattern = r'_item_linked\.child_name\s*\n((?:\s*[^\n]+\n)*)'
            
            parent_match = re.search(parent_pattern, block_text)
            child_match = re.search(child_pattern, block_text)
            
            if parent_match and child_match:
                parent_lines = parent_match.group(1).strip().split('\n')
                child_lines = child_match.group(1).strip().split('\n')
                
                for parent_line, child_line in zip(parent_lines, child_lines):
                    parent_name = parent_line.strip().strip('\'"')
                    child_name = child_line.strip().strip('\'"')
                    
                    if parent_name and child_name:
                        self._relationships.append({
                            'parent_name': parent_name,
                            'child_name': child_name,
                            'save_name': save_name
                        })
                        
    def _parse_xsd_schema(self):
        """Parse XSD schema to extract element/attribute requirements"""
        if not self.xsd_file:
            return
            
        try:
            tree = ET.parse(self.xsd_file)
            root = tree.getroot()
            
            # Find namespace - the XSD schema uses the xs namespace
            ns = {'xs': 'http://www.w3.org/2001/XMLSchema'}
            
            # Parse complex types to understand structure
            self._parse_complex_types(root, ns)
            
            # Parse global elements
            self._parse_elements(root, ns)
            
            print(f"✓ XSD schema parsed: {len(self._xsd_complex_types)} complex types, {len(self._xsd_elements)} elements")
            
        except Exception as e:
            print(f"⚠️ Warning: Could not parse XSD schema: {e}")
            
    def _parse_complex_types(self, root: ET.Element, ns: dict):
        """Parse complex types from XSD schema"""
        complex_types = root.findall('.//xs:complexType', ns)
        
        for ct in complex_types:
            type_name = ct.get('name')
            if not type_name:
                continue
                
            self._xsd_complex_types[type_name] = {
                'name': type_name,
                'elements': [],
                'attributes': [],
                'required_elements': []
            }
            
            # Parse sequence elements within the complex type
            sequences = ct.findall('.//xs:sequence', ns)
            for sequence in sequences:
                elements = sequence.findall('.//xs:element', ns)
                for elem in elements:
                    elem_name = elem.get('name')
                    if elem_name:
                        min_occurs = elem.get('minOccurs', '1')
                        max_occurs = elem.get('maxOccurs', '1')
                        elem_type = elem.get('type', '')
                        
                        elem_info = {
                            'name': elem_name,
                            'type': elem_type,
                            'min_occurs': min_occurs,
                            'max_occurs': max_occurs,
                            'required': min_occurs != '0'
                        }
                        
                        self._xsd_complex_types[type_name]['elements'].append(elem_info)
                        
                        # Track required elements separately
                        if min_occurs != '0':
                            self._xsd_complex_types[type_name]['required_elements'].append(elem_name)
                        
            # Parse attributes within complex type
            attributes = ct.findall('.//xs:attribute', ns)
            for attr in attributes:
                attr_name = attr.get('name')
                if attr_name:
                    use = attr.get('use', 'optional')
                    attr_type = attr.get('type', '')
                    
                    attr_info = {
                        'name': attr_name,
                        'type': attr_type,
                        'use': use,
                        'required': use == 'required'
                    }
                    
                    self._xsd_complex_types[type_name]['attributes'].append(attr_info)
                        
    def _parse_elements(self, root: ET.Element, ns: dict):
        """Parse element definitions from XSD schema"""
        elements = root.findall('.//xs:element', ns)
        
        for elem in elements:
            elem_name = elem.get('name')
            if not elem_name:
                continue
                
            self._xsd_elements[elem_name] = {
                'name': elem_name,
                'type': elem.get('type', ''),
                'min_occurs': elem.get('minOccurs', '1'),
                'max_occurs': elem.get('maxOccurs', '1'),
                'default': elem.get('default', ''),
                'fixed': elem.get('fixed', '')
            }
            
    @lru_cache(maxsize=128)
    def _generate_comprehensive_mapping(self) -> Dict[str, Any]:
        """Generate complete mapping rules that eliminate hardcoding needs"""
        mapping_rules = {
            "structural_mapping": {
                "root_element": "datablock",
                "root_attributes": ["datablockName"],
                "namespace": "http://pdbml.pdb.org/schema/pdbx-v50.xsd",
                "schema_location": "pdbx-v50.xsd"
            },
            "category_mapping": self._generate_category_mappings(),
            "item_mapping": self._generate_item_mappings(),
            "element_requirements": self._generate_element_requirements(),
            "attribute_requirements": self._generate_attribute_requirements(),
            "default_values": self._generate_default_values(),
            "validation_rules": self._generate_validation_rules(),
            "statistics": {
                "total_categories": len(self.categories),
                "total_items": len(self.items),
                "total_relationships": len(self.relationships),
                "total_enumerations": len(self.enumerations)
            }
        }
        
        return mapping_rules
        
    @lru_cache(maxsize=64)
    def _generate_category_mappings(self) -> Dict[str, Any]:
        """Generate category-level XML mappings"""
        category_mapping = {}
        
        for cat_id, cat_info in self.categories.items():
            # Determine XML mapping type based on keys
            keys = cat_info.get('keys', [])
            
            if not keys:
                xml_type = "root_child_element"
            elif len(keys) == 1:
                xml_type = "simple_element"
            else:
                xml_type = "composite_element"
                
            # Generate key attributes
            key_attributes = []
            for key in keys:
                key_attributes.append(f"_{cat_id}.{key}")
                
            category_mapping[cat_id] = {
                "xml_type": xml_type,
                "key_attributes": key_attributes,
                "grouping": "by_composite_key" if len(keys) > 1 else "by_single_key",
                "container": "entry" if xml_type == "root_child_element" else "category",
                "mandatory": cat_info.get('mandatory', 'no')
            }
            
        return category_mapping
        
    @lru_cache(maxsize=64)
    def _generate_item_mappings(self) -> Dict[str, Any]:
        """Generate item-level XML mappings"""
        item_mapping = {}
        
        for item_name, item_info in self.items.items():
            # Determine if item should be element or attribute
            xml_location = self._determine_xml_location(item_name, item_info)
            
            # Get data type information
            data_type = self._get_item_data_type(item_name)
            
            # Get default value
            default_value = self._get_item_default_value(item_name)
            
            # Get enumeration values
            enum_values = self.enumerations.get(item_name, [])
            
            item_mapping[item_name] = {
                "xml_location": xml_location,
                "data_type": data_type,
                "default_value": default_value,
                "enumeration_values": enum_values,
                "mandatory": item_info.get('mandatory', 'no'),
                "description": item_info.get('description', '')
            }
            
        return item_mapping
        
    def _determine_xml_location(self, item_name: str, item_info: dict) -> str:
        """Determine if item should be XML element or attribute"""
        # Note: item_info parameter kept for API compatibility but not currently used
        _ = item_info  # Explicitly mark as unused
        # Extract category and item parts
        if '.' not in item_name:
            return XMLLocation.ELEMENT_CONTENT.value
            
        category_part, item_part = item_name.split('.', 1)
        category_name = category_part.lstrip('_')
        
        # Check if this is a key item (should be attribute)
        if category_name in self.categories:
            keys = self.categories[category_name].get('keys', [])
            if item_part in keys:
                return XMLLocation.ATTRIBUTE.value
                
        # Special rules for known categories
        element_only_items = get_element_only_items(category_name)
        
        if element_only_items and item_part in element_only_items:
            return XMLLocation.ELEMENT_CONTENT.value
                
        # Default to element for most cases
        return XMLLocation.ELEMENT_CONTENT.value
        
    def _generate_element_requirements(self) -> Dict[str, List[str]]:
        """Generate element requirements mapping"""
        element_requirements = {}
        
        # Process each category
        for cat_id in self.categories:
            element_only = []
            
            # Get all items for this category
            category_items = [item for item in self.items 
                            if item.startswith(f'_{cat_id}.')]
            
            for item_name in category_items:
                item_part = item_name.split('.', 1)[1]
                xml_location = self._determine_xml_location(item_name, self.items[item_name])
                
                if xml_location == "element_content":
                    element_only.append(item_part)
                    
            if element_only:
                element_requirements[cat_id] = element_only
                
        return element_requirements
        
    def _generate_attribute_requirements(self) -> Dict[str, List[str]]:
        """Generate attribute requirements mapping"""
        attribute_requirements = {}
        
        # Process each category
        for cat_id in self.categories:
            attribute_only = []
            
            # Get all items for this category
            category_items = [item for item in self.items 
                            if item.startswith(f'_{cat_id}.')]
            
            for item_name in category_items:
                item_part = item_name.split('.', 1)[1]
                xml_location = self._determine_xml_location(item_name, self.items[item_name])
                
                if xml_location == "attribute":
                    attribute_only.append(item_part)
                    
            if attribute_only:
                attribute_requirements[cat_id] = attribute_only
                
        return attribute_requirements
        
    def _generate_default_values(self) -> Dict[str, Dict[str, str]]:
        """Generate default values mapping"""
        default_values = {}
        
        # Get defaults for atom_site category from schema-driven sources only
        atom_site_defaults = {}
        atom_site_defaults.update(get_atom_site_defaults())
        atom_site_defaults.update(get_anisotropic_defaults())
        
        # Note: No arbitrary hardcoded defaults added - let schema validation 
        # fail transparently to show real data issues
        
        default_values["atom_site"] = atom_site_defaults
        
        # Process all categories using schema-driven approach
        for cat_id in self.categories:
            category_defaults = {}
            
            # Get all items for this category
            category_items = [item for item in self.items 
                            if item.startswith(f'_{cat_id}.')]
            
            for item_name in category_items:
                item_part = item_name.split('.', 1)[1]
                
                # Get default value
                default_val = self._get_item_default_value(item_name)
                if default_val:
                    category_defaults[item_part] = default_val
                    
            if category_defaults:
                default_values[cat_id] = category_defaults
                
        return default_values
        
    def _generate_validation_rules(self) -> Dict[str, Dict[str, Any]]:
        """Generate validation rules mapping"""
        validation_rules = {}
        
        # Process each category
        for cat_id in self.categories:
            category_validation = {}
            
            # Get all items for this category
            category_items = [item for item in self.items 
                            if item.startswith(f'_{cat_id}.')]
            
            for item_name in category_items:
                item_part = item_name.split('.', 1)[1]
                
                # Get validation rules
                if item_name in self.enumerations:
                    category_validation[item_part] = {
                        "type": "enumeration",
                        "values": self.enumerations[item_name]
                    }
                    
            if category_validation:
                validation_rules[cat_id] = category_validation
                
        return validation_rules
        
    def _get_item_data_type(self, item_name: str) -> str:
        """Get data type for item"""
        if item_name in self.item_types:
            return self.item_types[item_name].get('primitive_code', 'char')
        return 'char'
        
    def _get_item_default_value(self, item_name: str) -> str:
        """Get default value for item from XSD schema only - no arbitrary defaults."""
        # Check XSD for default values
        if item_name in self.xsd_elements:
            return self.xsd_elements[item_name].get('default', '')
            
        # Return empty string - let schema validation handle missing values transparently
        return ''


class DictionaryParser:
    """Parser for mmCIF dictionary files to extract category and item metadata."""
    
    def __init__(self):
        self.categories: Dict[str, Dict[str, Any]] = {}
        self.items: Dict[str, Dict[str, Any]] = {}
        self.relationships: Dict[str, List[Dict[str, Any]]] = {}
        self.enumerations: Dict[str, List[str]] = {}
        
    def parse_dictionary(self, dict_path: Union[str, Path]) -> None:
        """Parse mmCIF dictionary file to extract metadata."""
        try:
            parser = MMCIFParser(validator_factory=None)
            container = parser.parse_file(dict_path)
            
            if not container.data:
                raise ValueError("No data blocks found in dictionary")
                
            dict_block = container.data[0]
            print(f"✓ Parsing dictionary with {len(dict_block.categories)} categories")
            
            # Parse category definitions first to initialize structure
            if "_category" in dict_block.categories:
                print("✓ Parsing category definitions...")
                self._parse_categories(dict_block["_category"])
                
            # Parse category key definitions (most important for key extraction)
            if "_category_key" in dict_block.categories:
                print("✓ Parsing category key definitions...")
                self._parse_category_keys(dict_block["_category_key"])
                
            # Parse item definitions  
            if "_item" in dict_block.categories:
                print("✓ Parsing item definitions...")
                self._parse_items(dict_block["_item"])
                
            # Parse item type definitions
            if "_item_type" in dict_block.categories:
                print("✓ Parsing item type definitions...")
                self._parse_item_types(dict_block["_item_type"])
                
            # Parse enumeration definitions
            if "_item_enumeration" in dict_block.categories:
                print("✓ Parsing enumeration definitions...")
                self._parse_enumerations(dict_block["_item_enumeration"])
                
            # Parse relationships/links
            if "_item_linked" in dict_block.categories:
                print("✓ Parsing relationship definitions...")
                self._parse_relationships(dict_block["_item_linked"])
                
            # Print summary
            categories_with_keys = sum(1 for cat in self.categories.values() if cat.get("keys"))
            print("✓ Dictionary parsing complete:")
            print(f"  - Categories: {len(self.categories)}")
            print(f"  - Categories with keys: {categories_with_keys}")
            print(f"  - Items: {len(self.items)}")
            print(f"  - Enumerations: {len(self.enumerations)}")
            print(f"  - Relationships: {len(self.relationships)}")
            
        except Exception as e:
            print(f"⚠️ Error parsing dictionary: {e}")
            # Initialize empty structures on error
            self.categories = {}
            self.items = {}
            self.relationships = {}
            self.enumerations = {}
            raise
    
    def _parse_categories(self, category: Category) -> None:
        """Parse category definitions."""
        data = category.data
        if "id" in data:
            for i, cat_id in enumerate(data["id"]):
                self.categories[cat_id] = {
                    "id": cat_id,
                    "description": data.get("description", [None] * len(data["id"]))[i] or "",
                    "mandatory_code": data.get("mandatory_code", [None] * len(data["id"]))[i] or "no",
                    "keys": []  # Initialize empty keys list
                }
    
    def _parse_items(self, category: Category) -> None:
        """Parse item definitions."""
        data = category.data
        if "name" in data:
            for i, item_name in enumerate(data["name"]):
                self.items[item_name] = {
                    "name": item_name,
                    "category_id": data.get("category_id", [None] * len(data["name"]))[i] or "",
                    "mandatory_code": data.get("mandatory_code", [None] * len(data["name"]))[i] or "no",
                    "description": data.get("description", [None] * len(data["name"]))[i] or "",
                    "type_code": data.get("type_code", [None] * len(data["name"]))[i] or ""
                }
    
    def _parse_item_types(self, category: Category) -> None:
        """Parse item type definitions."""
        data = category.data
        if "code" in data:
            for i, type_code in enumerate(data["code"]):
                # Create type information structure
                type_info = {
                    "type_code": type_code,
                    "primitive_code": data.get("primitive_code", [None] * len(data["code"]))[i] or "",
                    "construct": data.get("construct", [None] * len(data["code"]))[i] or "",
                    "detail": data.get("detail", [None] * len(data["code"]))[i] or ""
                }
                
                # Find items that use this type code and update them
                for _, item_info in self.items.items():
                    if item_info.get("type_code") == type_code:
                        item_info.update(type_info)
    
    def _parse_enumerations(self, category: Category) -> None:
        """Parse enumeration definitions."""
        data = category.data
        if "name" in data and "value" in data:
            for i, item_name in enumerate(data["name"]):
                if item_name not in self.enumerations:
                    self.enumerations[item_name] = []
                self.enumerations[item_name].append(data["value"][i])
    
    def _parse_relationships(self, category: Category) -> None:
        """Parse item relationship/link definitions."""
        data = category.data
        if "child_name" in data and "parent_name" in data:
            for i, child_name in enumerate(data["child_name"]):
                if child_name not in self.relationships:
                    self.relationships[child_name] = []
                self.relationships[child_name].append({
                    "parent_name": data["parent_name"][i],
                    "child_name": child_name
                })
    
    def _parse_category_keys(self, category: Category) -> None:
        """Extract key items from _category_key - this is the most critical part for proper key detection."""
        data = category.data
        if "name" in data:
            for full_item_name in data["name"]:
                # Parse category.item format (e.g., "_citation.id" -> category="citation", item="id")
                if "." in full_item_name:
                    # Remove leading underscore and split
                    clean_name = full_item_name.lstrip("_")
                    if "." in clean_name:
                        cat_name, item_name = clean_name.split(".", 1)
                        
                        # Ensure category exists in our categories dict
                        if cat_name not in self.categories:
                            self.categories[cat_name] = {
                                "id": cat_name,
                                "description": "",
                                "mandatory_code": "no",
                                "keys": []
                            }
                        
                        # Ensure keys list exists
                        if "keys" not in self.categories[cat_name]:
                            self.categories[cat_name]["keys"] = []
                        
                        # Add the key item if not already present
                        if item_name not in self.categories[cat_name]["keys"]:
                            self.categories[cat_name]["keys"].append(item_name)
                            print(f"✓ Found key for category '{cat_name}': '{item_name}'")
                else:
                    print(f"⚠️ Warning: Invalid key format in _category_key: '{full_item_name}'")
    
    def get_category_key_items(self, category_name: str) -> List[str]:
        """Get the key items for a category from _category_key definitions."""
        # Remove leading underscore for lookup
        clean_category = category_name.lstrip('_')
        
        # First, try to get keys from _category_key definitions
        if clean_category in self.categories and "keys" in self.categories[clean_category]:
            keys = self.categories[clean_category]["keys"]
            if keys:
                print(f"✓ Found {len(keys)} key(s) for category '{clean_category}': {keys}")
                return keys
        
        # Debug: Show what categories we do have
        available_cats = list(self.categories)
        print(f"⚠️ No keys found for category '{clean_category}'")
        print(f"   Available categories: {available_cats[:10]}{'...' if len(available_cats) > 10 else ''}")
        
        # Fallback: No keys found in dictionary
        return []
    
    def get_parent_relationships(self, child_category: str) -> List[Dict[str, str]]:
        """Get parent relationships for a child category."""
        relationships = []
        for item_name, links in self.relationships.items():
            if item_name.startswith(f"_{child_category}."):
                relationships.extend(links)
        return relationships


class PDBMLConverter:
    """Convert mmCIF data to PDBML XML format with optimized performance."""
    
    def __init__(self, dictionary_path: Optional[Union[str, Path]] = None, cache_dir: Optional[str] = None, 
                 permissive: bool = False):
        """Initialize converter with optional dictionary for metadata.
        
        Args:
            dictionary_path: Path to mmCIF dictionary file
            cache_dir: Directory for caching mapping rules
            permissive: If True, attempt to add missing required schema elements 
                       based on XSD schema analysis (when available). 
                       Default False - let validation fail transparently to show true data issues.
                       Note: Only adds elements if XSD schema parsing is available and functional.
        """
        self.cache_dir = cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
        self.permissive = permissive
        
        # Set default dictionary path if not provided
        if dictionary_path is None:
            dictionary_path = Path(__file__).parent / "schemas" / "mmcif_pdbx_v50.dic"
        
        # Lazy-load dictionary parser only when needed
        self._dictionary = None
        self._dictionary_path = dictionary_path
        
        # Initialize embedded XML mapping generator with caching
        xsd_path = Path(__file__).parent / "schemas" / "pdbx-v50.xsd"
        self.mapping_generator = XMLMappingGenerator(
            dict_file=dictionary_path,
            xsd_file=xsd_path if xsd_path.exists() else None,
            cache_dir=self.cache_dir
        )
        
        # Lazy-load mapping rules
        self._mapping_rules = None
        
        # Lazy-load XML validator
        self._xml_validator = None
        
        # PDBML namespace
        self.namespace = "http://pdbml.pdb.org/schema/pdbx-v50.xsd"
        self.ns_prefix = "PDBx"
        
        # Cache for frequently used data
        self._category_keys_cache = {}
        self._element_only_items_cache = None
        self._attribute_only_items_cache = None
        
    @property
    def dictionary(self) -> Optional[DictionaryParser]:
        """Lazy-loaded dictionary property."""
        if self._dictionary is None and self._dictionary_path:
            self._dictionary = DictionaryParser()
            self._dictionary.parse_dictionary(self._dictionary_path)
        return self._dictionary
        
    @property
    def mapping_rules(self) -> Dict[str, Any]:
        """Lazy-loaded mapping rules property."""
        if self._mapping_rules is None:
            self._mapping_rules = self.mapping_generator.get_mapping_rules()
        return self._mapping_rules
        
    @property
    def xml_validator(self) -> Optional:
        """Lazy-loaded XML validator property."""
        if self._xml_validator is None:
            self._xml_validator = self._initialize_xml_validator()
        return self._xml_validator
        
    def convert_to_pdbml(self, mmcif_container: MMCIFDataContainer) -> str:
        """Convert mmCIF container to PDBML XML string."""
        if len(mmcif_container.data) != 1:
            raise ValueError("PDBML conversion requires exactly one data block")
        
        data_block = mmcif_container.data[0]
        
        # Create root datablock element with namespace
        root = ET.Element("datablock")
        root.set("datablockName", data_block.name)
        
        # Set up namespace attributes carefully to avoid duplicates
        root.set("xmlns", self.namespace)
        root.set("{http://www.w3.org/2001/XMLSchema-instance}schemaLocation", 
                f"{self.namespace} pdbx-v50.xsd")
        
        # Process each category in the data block
        for category_name in data_block.categories:
            try:
                category = data_block[category_name]
                self._add_category_to_pdbml(root, category_name, category)
            except Exception as e:
                print(f"⚠️ Error processing category {category_name}: {str(e)}")
                # Continue with other categories
        
        # Convert to pretty XML string with robust error handling
        try:
            rough_string = ET.tostring(root, encoding='utf-8')
            
            # Write raw XML to file for debugging
            with open("debug_raw_xml.xml", "wb") as f:
                f.write(rough_string)
                
            # Generate XML output without fallback defaults
            return self._generate_simple_xml_output(root)
            
        except Exception as e:
            print(f"⚠️ Error generating XML: {str(e)}")
            # Instead of generating fallback with defaults, let it fail transparently
            raise ValueError(f"XML generation failed: {str(e)}. Check source data quality.")
            
    def _generate_simple_xml_output(self, root: ET.Element) -> str:
        """Generate XML output without adding default values - let validation show real issues."""
        # Only check for critical categories that are absolutely required for valid XML structure
        self._validate_critical_references(root)
            
        # Use ElementTree's built-in serialization without pretty-printing
        xml_string = ET.tostring(root, encoding='utf-8').decode('utf-8')
        return '<?xml version="1.0" encoding="utf-8"?>\n' + xml_string
        
    def _validate_critical_references(self, root: ET.Element) -> None:
        """Validate critical references without auto-adding defaults - report issues instead."""
        issues = []
        
        # Check for atom_type category
        atom_type_cat = None
        atom_site_cat = None
        atom_symbols = set()
        
        for cat in root:
            if cat.tag.endswith("atom_typeCategory"):
                atom_type_cat = cat
            elif cat.tag.endswith("atom_siteCategory"):
                atom_site_cat = cat
                # Extract the type_symbol values
                for atom_elem in cat:
                    symbol = None
                    for child in atom_elem:
                        if child.tag.endswith("type_symbol"):
                            symbol = child.text
                            break
                    if not symbol:
                        symbol = atom_elem.get("type_symbol")
                    if symbol:
                        atom_symbols.add(symbol)
        
        # Report missing atom_type references
        if atom_site_cat and atom_symbols and not atom_type_cat:
            issues.append(f"Missing atom_type category for symbols: {', '.join(sorted(atom_symbols))}")
        
        # Check for entity/struct_asym requirements
        entity_ids = set()
        asym_ids = set()
        
        if atom_site_cat:
            for atom_elem in atom_site_cat:
                entity_id = asym_id = None
                for child in atom_elem:
                    if child.tag.endswith("label_entity_id"):
                        entity_id = child.text
                    elif child.tag.endswith("label_asym_id"):
                        asym_id = child.text
                
                if entity_id:
                    entity_ids.add(entity_id)
                if asym_id:
                    asym_ids.add(asym_id)
        
        # Check for missing entity category
        entity_cat = any(cat.tag.endswith("entityCategory") for cat in root)
        if entity_ids and not entity_cat:
            issues.append(f"Missing entity category for entity_ids: {', '.join(sorted(entity_ids))}")
        
        # Check for missing struct_asym category
        struct_asym_cat = any(cat.tag.endswith("struct_asymCategory") for cat in root)
        if asym_ids and not struct_asym_cat:
            issues.append(f"Missing struct_asym category for asym_ids: {', '.join(sorted(asym_ids))}")
        
        # Report all issues without fixing them
        if issues:
            print("⚠️ Validation issues detected (not auto-fixed):")
            for issue in issues:
                print(f"  - {issue}")
            print("  Note: Run with proper validation to see detailed schema compliance report.")
    
    def _add_category_to_pdbml(self, parent: ET.Element, category_name: str, category: Category) -> None:
        """Add a category to the PDBML XML structure."""
        try:
            # Convert mmCIF category name to PDBML format
            pdbml_category_name = self._mmcif_to_pdbml_category_name(category_name)
            
            # Validate XML element name
            pdbml_category_name = self._sanitize_xml_name(pdbml_category_name)
            category_elem = ET.SubElement(parent, f"{pdbml_category_name}Category")
            
            # Get category data
            data = category.data
            if not data:
                return
            
            # Determine if this is a single-row or multi-row category
            row_count = max(len(values) for values in data.values()) if data else 0
            
            # Get key items for this category
            key_items = list(self._get_category_keys(category_name))                
            # Add common keys if none were found in the dictionary
            if not key_items:
                # Use mapping rules instead of hardcoded values
                key_items = self._get_keys_from_mapping_rules(category_name)
                if not key_items:
                    # Final fallback for essential categories using Enum
                    key_items = EssentialKey.get_keys(category_name)
            
            # Create elements for each row
            for row_idx in range(row_count):
                row_elem = ET.SubElement(category_elem, pdbml_category_name)
                
                # Add key items as attributes (avoid duplicates) - FIX: Process ALL key items for composite keys
                added_attrs = set()
                for key_item in key_items:
                    if key_item in data and row_idx < len(data[key_item]) and key_item not in added_attrs:
                        cleaned_value = self._clean_field_value(str(data[key_item][row_idx]), key_item)
                        attr_name = self._sanitize_xml_name(key_item)
                        if attr_name:  # Only add valid attribute names
                            row_elem.set(attr_name, cleaned_value)
                            added_attrs.add(key_item)
                
                # Handle required attributes based on schema, not hardcoded category checks
                required_attrs_for_category = RequiredAttribute.get_required_attrs(pdbml_category_name)
                if required_attrs_for_category:
                    for attr_name in required_attrs_for_category:
                        if attr_name in data and row_idx < len(data[attr_name]) and attr_name not in added_attrs:
                            cleaned_value = self._clean_field_value(str(data[attr_name][row_idx]), attr_name)
                            if cleaned_value:  # Only add non-empty values
                                attr_xml_name = self._sanitize_xml_name(attr_name)
                                row_elem.set(attr_xml_name, cleaned_value)
                                added_attrs.add(attr_name)
                
                # Define items that MUST be attributes (not elements) according to the schema
                attr_only_items = self._get_attribute_only_items_from_mapping()
                
                # Define items that MUST be elements (not attributes) according to the schema
                element_only_items = self._get_element_only_items_from_mapping()
                
                # Get list of attributes that should not be elements for this category
                force_as_attrs = attr_only_items.get(pdbml_category_name, [])
                
                # Get list of items that should be elements not attributes for this category
                force_as_elems = element_only_items.get(pdbml_category_name, [])
                
                # Add non-key items as child elements
                for item_name, values in data.items():
                    if item_name not in key_items and row_idx < len(values):
                        # Skip items that should be attributes based on schema mapping
                        if item_name in force_as_attrs:
                            continue
                            
                        # Handle 'id' field based on schema requirements rather than hardcoded categories
                        if item_name == "id":
                            # Check if this should be an attribute based on mapping rules or requirements
                            if "id" not in added_attrs:
                                # Use RequiredAttribute to determine if 'id' should be an attribute for this category
                                required_attrs = RequiredAttribute.get_required_attrs(pdbml_category_name)
                                if required_attrs and "id" in required_attrs:
                                    cleaned_value = self._clean_field_value(str(values[row_idx]), item_name)
                                    row_elem.set(item_name, cleaned_value)
                                    continue
                                
                        # Handle items that must be attributes according to schema
                        if item_name in force_as_attrs:
                            if item_name not in added_attrs:
                                cleaned_value = self._clean_field_value(str(values[row_idx]), item_name)
                                if cleaned_value:  # Only add non-empty values
                                    row_elem.set(item_name, cleaned_value)
                                    added_attrs.add(item_name)
                            continue
                        
                        # Handle schema-driven element vs attribute decisions
                        if (pdbml_category_name in force_as_elems and item_name in force_as_elems[pdbml_category_name]):
                            safe_item_name = self._sanitize_xml_name(item_name)
                            try:
                                cleaned_value = self._clean_field_value(str(values[row_idx]), item_name)
                                # Always add the element, even if value is empty (preserves structure)
                                item_elem = ET.SubElement(row_elem, safe_item_name)
                                item_elem.text = cleaned_value if cleaned_value else ""
                            except Exception as e:
                                print(f"⚠️ Error adding element '{safe_item_name}': {str(e)}")
                            continue
                        
                        # Make sure the element name is valid XML
                        safe_item_name = self._sanitize_xml_name(item_name)
                        if safe_item_name:  # Skip invalid element names
                            try:
                                cleaned_value = self._clean_field_value(str(values[row_idx]), item_name)
                                # Always add the element, even if value is empty (preserves structure)
                                item_elem = ET.SubElement(row_elem, safe_item_name)
                                item_elem.text = cleaned_value if cleaned_value else ""
                            except Exception as e:
                                print(f"⚠️ Error adding element '{safe_item_name}': {str(e)}")
                
                # Handle schema validation and permissive mode additions - generalized approach
                if self.permissive:
                    self._add_schema_required_elements(row_elem, pdbml_category_name)
                
                # Category-specific validation reporting (schema-driven)
                self._validate_category_completeness(row_elem, pdbml_category_name, data)
                
        except Exception as e:
            print(f"⚠️ Error processing category {category_name}: {str(e)}")
    
    def _validate_category_completeness(self, row_elem: ET.Element, category_name: str, data: Dict[str, List[Any]]) -> None:
        """Validate category completeness and report issues without auto-fixing.
        
        This method performs schema-driven validation to check for missing required elements,
        invalid references, and data integrity issues. It reports problems transparently
        without adding arbitrary defaults or fixes.
        
        Args:
            row_elem: The XML element representing a row in the category
            category_name: The name of the category (e.g., "atom_site", "citation", etc.)
            data: The raw category data from mmCIF
        """
        if not row_elem or not category_name:
            return
            
        issues = []
        warnings = []
        
        # 1. Check for missing required elements based on XSD schema
        required_elements = self._get_required_elements_from_xsd(category_name)
        if required_elements:
            missing_required = []
            for elem_name in required_elements:
                # Check if element exists in XML
                existing_elem = row_elem.find(elem_name)
                if existing_elem is None:
                    # Check if it exists as an attribute
                    if elem_name not in row_elem.attrib:
                        missing_required.append(elem_name)
            
            if missing_required:
                issues.append(f"Missing required elements for {category_name}: {', '.join(missing_required)}")
        
        # 2. Check for missing required attributes based on schema
        required_attrs = RequiredAttribute.get_required_attrs(category_name)
        if required_attrs:
            missing_attrs = []
            for attr_name in required_attrs:
                if attr_name not in row_elem.attrib:
                    # Check if it exists in source data but wasn't added
                    if attr_name in data:
                        warnings.append(f"Required attribute '{attr_name}' exists in data but not added to XML for {category_name}")
                    else:
                        missing_attrs.append(attr_name)
            
            if missing_attrs:
                issues.append(f"Missing required attributes for {category_name}: {', '.join(missing_attrs)}")
        
        # 3. Validate referential integrity for key relationships
        integrity_issues = self._check_referential_integrity(row_elem, category_name, data)
        if integrity_issues:
            issues.extend(integrity_issues)
        
        # 4. Check for enum validation issues
        enum_issues = self._check_enumeration_compliance(row_elem, category_name)
        if enum_issues:
            warnings.extend(enum_issues)
        
        # 5. Check for data type compliance
        type_issues = self._check_data_type_compliance(row_elem, category_name)
        if type_issues:
            warnings.extend(type_issues)
        
        # Report issues without fixing them
        if issues:
            print(f"⚠️ Validation issues in {category_name}:")
            for issue in issues:
                print(f"  - {issue}")
        
        if warnings:
            print(f"💡 Validation warnings in {category_name}:")
            for warning in warnings:
                print(f"  - {warning}")
        
        # In permissive mode, note what would be added
        if self.permissive and issues:
            print(f"  Note: Permissive mode will attempt to add missing schema-required elements with null indicators")

    def _check_referential_integrity(self, row_elem: ET.Element, category_name: str, data: Dict[str, List[Any]]) -> List[str]:
        """Check for referential integrity issues without auto-fixing."""
        issues = []
        
        # Category-specific integrity checks based on mmCIF standards
        if category_name == "atom_site":
            # Check for atom_type references
            type_symbol = row_elem.get("type_symbol")
            if not type_symbol:
                # Check in child elements
                type_symbol_elem = row_elem.find("type_symbol")
                if type_symbol_elem is not None:
                    type_symbol = type_symbol_elem.text
            
            if type_symbol:
                # This would require access to the full XML tree to check atom_type category
                # For now, just note the potential issue
                issues.append(f"type_symbol '{type_symbol}' should have corresponding atom_type entry")
            
            # Check for entity references
            entity_id = row_elem.get("label_entity_id")
            if not entity_id:
                entity_id_elem = row_elem.find("label_entity_id")
                if entity_id_elem is not None:
                    entity_id = entity_id_elem.text
            
            if entity_id:
                issues.append(f"label_entity_id '{entity_id}' should have corresponding entity entry")
            
            # Check for struct_asym references
            asym_id = row_elem.get("label_asym_id")
            if not asym_id:
                asym_id_elem = row_elem.find("label_asym_id")
                if asym_id_elem is not None:
                    asym_id = asym_id_elem.text
            
            if asym_id:
                issues.append(f"label_asym_id '{asym_id}' should have corresponding struct_asym entry")
        
        elif category_name == "citation_author":
            # Check for citation reference
            citation_id = row_elem.get("citation_id")
            if citation_id:
                issues.append(f"citation_id '{citation_id}' should have corresponding citation entry")
        
        elif category_name == "entity_poly_seq":
            # Check for entity reference
            entity_id = row_elem.get("entity_id")
            if entity_id:
                issues.append(f"entity_id '{entity_id}' should have corresponding entity entry")
        
        # Add more category-specific checks as needed
        
        return issues

    def _check_enumeration_compliance(self, row_elem: ET.Element, category_name: str) -> List[str]:
        """Check for enumeration compliance issues."""
        warnings = []
        
        # Get enumeration rules from mapping rules if available
        if self.mapping_rules:
            validation_rules = self.mapping_rules.get("validation_rules", {})
            if category_name in validation_rules:
                category_rules = validation_rules[category_name]
                
                for item_name, rule_info in category_rules.items():
                    if rule_info.get("type") == "enumeration":
                        valid_values = rule_info.get("values", [])
                        
                        # Extract just the value part (before any quotes or descriptions) for comparison
                        clean_valid_values = []
                        for val in valid_values:
                            # Handle different enumeration value formats
                            if '"' in val:
                                # Has double quote description: "value description"
                                clean_val = val.split('"')[0].rstrip()
                            elif "'" in val and val.count("'") == 1:
                                # Has single quote description: "value  'description"
                                clean_val = val.split("'")[0].rstrip()
                            else:
                                # Check if it has a description after multiple spaces
                                # Look for pattern like "N  No" where we want "N  "
                                parts = val.split('  ')  # Split on double space
                                if len(parts) > 1 and parts[1].strip():
                                    # Has description after double space, take first part + double space
                                    clean_val = parts[0] + '  '
                                else:
                                    # No description, use as-is
                                    clean_val = val
                            clean_valid_values.append(clean_val)
                        
                        # Check attribute value
                        attr_value = row_elem.get(item_name)
                        if attr_value:
                            if attr_value not in clean_valid_values:
                                warnings.append(f"Invalid enumeration value '{attr_value}' for {item_name}, valid values: {clean_valid_values}")
                        
                        # Check element value
                        elem = row_elem.find(item_name)
                        if elem is not None and elem.text:
                            if elem.text not in clean_valid_values:
                                warnings.append(f"Invalid enumeration value '{elem.text}' for {item_name}, valid values: {clean_valid_values}")
        
        return warnings

    def _check_data_type_compliance(self, row_elem: ET.Element, category_name: str) -> List[str]:
        """Check for data type compliance issues."""
        warnings = []
        
        # Get item mappings from mapping rules if available
        if self.mapping_rules:
            item_mapping = self.mapping_rules.get("item_mapping", {})
            
            # Check all attributes and elements for type compliance
            for attr_name, attr_value in row_elem.attrib.items():
                full_item_name = f"_{category_name}.{attr_name}"
                if full_item_name in item_mapping:
                    data_type = item_mapping[full_item_name].get("data_type", "")
                    if not self._validate_data_type(attr_value, data_type):
                        warnings.append(f"Data type mismatch for {attr_name}: expected {data_type}, got '{attr_value}'")
            
            # Check child elements
            for elem in row_elem:
                if elem.text:
                    full_item_name = f"_{category_name}.{elem.tag}"
                    if full_item_name in item_mapping:
                        data_type = item_mapping[full_item_name].get("data_type", "")
                        if not self._validate_data_type(elem.text, data_type):
                            warnings.append(f"Data type mismatch for {elem.tag}: expected {data_type}, got '{elem.text}'")
        
        return warnings

    def _validate_data_type(self, value: str, expected_type: str) -> bool:
        """Validate if a value matches the expected data type."""
        if not value or not expected_type:
            return True  # Skip validation if no value or type info
        
        expected_type = expected_type.lower()
        
        try:
            if expected_type in ['int', 'integer']:
                int(value)
                return True
            elif expected_type in ['float', 'real', 'decimal']:
                float(value)
                return True
            elif expected_type in ['char', 'string', 'text']:
                return True  # Any string is valid for char/string types
            else:
                return True  # Unknown type, assume valid
        except (ValueError, TypeError):
            return False

    def _sanitize_xml_name(self, name: str) -> str:
        """Sanitize a name to be a valid XML element or attribute name."""
        # XML names must start with a letter, underscore, or colon
        # and can contain letters, digits, underscores, hyphens, periods, and colons
        
        # First, remove any characters that aren't valid in XML names
        name = re.sub(r'[^\w\-\.]', '_', name)
        
        # If it doesn't start with a valid character, prepend 'x_'
        if not name or not re.match(r'^[a-zA-Z_:]', name):
            name = 'x_' + name
            
        return name
    
    def _clean_field_value(self, value: str, field_name: str) -> str:
        """Clean field values for proper XML representation."""
        if not value:
            return ""
        
        try:
            # Remove surrounding quotes for certain fields that should be raw values
            numeric_fields = get_numeric_fields()
            if field_name in numeric_fields and value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            
            # Remove quotes from string fields that shouldn't have them
            elif value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            
            # Special case for null values represented in mmCIF
            if is_null_value(value):
                return ""
            
            # Handle problematic fields using Enum classes
            replacement = get_problematic_field_replacement(field_name, value)
            if replacement != value:
                return replacement
                
            # Remove/replace invalid XML characters
            # Replace control chars except for whitespace (\n, \t, etc.)
            result = ""
            for char in value:
                # Allow basic whitespace characters
                if char in ' \t\n\r':
                    result += char
                # Skip control characters and other invalid XML chars
                elif ord(char) < 32 and char not in '\t\n\r':
                    pass
                else:
                    result += char
                    
            # Escape special XML characters
            result = (result
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&apos;'))
                
            return result
        except Exception as e:
            print(f"⚠️ Warning: Error cleaning value '{value}': {str(e)}")
            return ""
    
    def _mmcif_to_pdbml_category_name(self, mmcif_name: str) -> str:
        """Convert mmCIF category name to PDBML format."""
        # Remove leading underscore
        name = mmcif_name.lstrip('_')
        # For PDBML, we need to keep underscores as they appear in the schema
        # The schema uses the exact category names like "database_2", "atom_site", etc.
        return name
    
    @lru_cache(maxsize=128)
    def _get_category_keys(self, category_name: str) -> tuple:
        """Get key items for a category using data-driven sources only (cached)."""
        # Remove leading underscore for lookup
        clean_category = category_name.lstrip('_')
        
        # Check cache first
        if clean_category in self._category_keys_cache:
            return tuple(self._category_keys_cache[clean_category])
        
        # First, try XML mapping rules (most accurate)
        if self.mapping_rules:
            category_mapping = self.mapping_rules.get("category_mapping", {})
            if clean_category in category_mapping:
                # Try key_attributes (plural) first
                key_attributes = category_mapping[clean_category].get("key_attributes", [])
                
                # If not found, check for key_attribute (singular)
                if not key_attributes and "key_attribute" in category_mapping[clean_category]:
                    key_attr = category_mapping[clean_category]["key_attribute"]
                    key_attributes = [key_attr] if key_attr else []
                
                # Extract just the item names (remove category prefix)
                keys = []
                for attr in key_attributes:
                    if attr.startswith(f"_{clean_category}."):
                        item_name = attr[len(f"_{clean_category}."):]
                        keys.append(item_name)
                if keys:
                    self._category_keys_cache[clean_category] = keys
                    return tuple(keys)
        
        # Second, try dictionary parser (if available)
        if self.dictionary:
            dict_keys = self.dictionary.get_category_key_items(category_name)
            if dict_keys:
                self._category_keys_cache[clean_category] = dict_keys
                return tuple(dict_keys)
        
        # No keys found - log warning and return empty tuple
        print(f"⚠️ No key items found for category {category_name}")
        self._category_keys_cache[clean_category] = []
        return tuple()
    
    def _get_element_only_items_from_mapping(self) -> Dict[str, List[str]]:
        """Get element-only items from mapping rules (cached)."""
        if self._element_only_items_cache is not None:
            return self._element_only_items_cache
            
        if not self.mapping_rules:
            # No mapping rules available - return empty dict
            self._element_only_items_cache = {}
        else:
            element_requirements = self.mapping_rules.get("element_requirements", {})
            self._element_only_items_cache = element_requirements
            
        return self._element_only_items_cache
    
    def _get_attribute_only_items_from_mapping(self) -> Dict[str, List[str]]:
        """Get attribute-only items from mapping rules (cached)."""
        if self._attribute_only_items_cache is not None:
            return self._attribute_only_items_cache
            
        if not self.mapping_rules:
            # No mapping rules available - return empty dict
            self._attribute_only_items_cache = {}
        else:
            attribute_requirements = self.mapping_rules.get("attribute_requirements", {})
            self._attribute_only_items_cache = attribute_requirements
            
        return self._attribute_only_items_cache
        
    def _get_keys_from_mapping_rules(self, category_name: str) -> List[str]:
        """Get key items from mapping rules if available."""
        if not self.mapping_rules:
            return []
            
        clean_category = category_name.lstrip('_')
        category_mapping = self.mapping_rules.get("category_mapping", {})
        
        if clean_category in category_mapping:
            key_attributes = category_mapping[clean_category].get("key_attributes", [])
            keys = []
            for attr in key_attributes:
                if attr.startswith(f"_{clean_category}."):
                    item_name = attr[len(f"_{clean_category}."):]
                    keys.append(item_name)
            return keys
        
        return []
    
    def _get_default_values_from_mapping(self, category_name: str) -> Dict[str, str]:
        """Get default values for a category from mapping rules."""
        if not self.mapping_rules:
            return {}
            
        clean_category = category_name.lstrip('_')
        default_values = self.mapping_rules.get("default_values", {})
        return default_values.get(clean_category, {})
    
    def _initialize_xml_validator(self) -> Optional[XMLSchemaValidator]:
        """Initialize XML Schema validator for PDBML validation."""
        # Look for the PDBML XSD schema file
        schema_path = Path(__file__).parent / "schemas" / "pdbx-v50.xsd"
        if schema_path.exists():
            try:
                validator = XMLSchemaValidator(str(schema_path))
                return validator
            except Exception as e:
                print(f"⚠️ Warning: Could not initialize XML validator: {e}")
        else:
            print(f"⚠️ Warning: PDBML XSD schema not found at {schema_path}")
        return None
    
    def validate_pdbml_xml(self, xml_content: str) -> Dict[str, Any]:
        """
        Validate PDBML XML content against the official XSD schema.
        
        Args:
            xml_content: PDBML XML content as string
            
        Returns:
            Dict containing validation results:
            - is_valid: bool indicating if validation passed
            - errors: list of validation errors
            - warnings: list of validation warnings
        """
        if not self.xml_validator:
            return {
                "is_valid": False,
                "errors": ["XML validator not available - XSD schema not found"],
                "warnings": []
            }
        
        try:
            # Use the existing XMLSchemaValidator
            result = self.xml_validator.validate(xml_content)
            return {
                "is_valid": result.get("valid", False),
                "errors": result.get("errors", []),
                "warnings": []  # XMLSchemaValidator doesn't separate warnings
            }
        except Exception as e:
            return {
                "is_valid": False,
                "errors": [f"Validation failed: {str(e)}"],
                "warnings": []
            }
    
    def convert_and_validate_pdbml(self, mmcif_container: MMCIFDataContainer) -> Dict[str, Any]:
        """
        Convert mmCIF to PDBML XML and validate against official schema.
        
        Args:
            mmcif_container: MMCIFDataContainer to convert
            
       
            
        Returns:
            Dict containing:
            - pdbml_xml: converted XML content
            - validation: validation results
        """
        # Step 1: Convert to PDBML XML
        pdbml_xml = self.convert_to_pdbml(mmcif_container)
        
        # Step 2: Validate against official schema
        validation_result = self.validate_pdbml_xml(pdbml_xml)
        
        return {
            "pdbml_xml": pdbml_xml,
            "validation": validation_result
        }
    
    def _fix_common_validation_issues(self, xml_content: str) -> str:
        """
        Fix common validation issues in PDBML XML to improve schema compliance.
        
        Args:
            xml_content: Original PDBML XML content
            
        Returns:
            Fixed PDBML XML content
        """
        try:
            # Parse the XML to work with it
            root = ET.fromstring(xml_content)
            
            # Fix 1: Ensure proper namespace declarations
            if "xmlns" not in root.attrib:
                root.set("xmlns", self.namespace)
            if "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation" not in root.attrib:
                root.set("{http://www.w3.org/2001/XMLSchema-instance}schemaLocation", 
                        f"{self.namespace} pdbx-v50.xsd")
            
            # Fix 2: Ensure datablock has proper attributes
            if "datablockName" not in root.attrib and root.tag == "datablock":
                root.set("datablockName", "UNKNOWN")
            
            # Fix 3: Remove empty categories that might cause validation issues
            categories_to_remove = []
            for category in root:
                if len(category) == 0:  # Empty category
                    categories_to_remove.append(category)
            
            for category in categories_to_remove:
                root.remove(category)
            
            # Fix 4: Ensure required elements have proper content
            self._ensure_required_elements_for_validation(root)
            
            # Convert back to string
            fixed_xml = ET.tostring(root, encoding='utf-8').decode('utf-8')
            return '<?xml version="1.0" encoding="utf-8"?>\n' + fixed_xml
            
        except Exception as e:
            print(f"⚠️ Warning: Could not fix validation issues: {e}")
            return xml_content
    
    def _ensure_required_elements_for_validation(self, root: ET.Element) -> None:
        """Ensure required elements are present for successful validation."""
        # This method can be expanded based on specific validation errors encountered
        # For now, it ensures basic structure compliance
        
        # Ensure we have at least one category with content
        has_content = False
        for category in root:
            if len(category) > 0:
                has_content = True
                break
        
        if not has_content:
            # Add minimal entry category for validation
            entry_cat = ET.SubElement(root, "entryCategory")
            entry_elem = ET.SubElement(entry_cat, "entry")
            entry_elem.set("id", root.get("datablockName", "UNKNOWN"))
    
    def _add_schema_required_elements(self, row_elem: ET.Element, category_name: str) -> None:
        """Add elements for missing required schema fields in permissive mode.
        
        This method queries the XSD schema to determine what elements are actually required
        for this category, then adds only those missing elements using appropriate null indicators.
        No hardcoded lists - everything comes from the schema definition.
        
        Args:
            row_elem: The XML element representing a row in the category
            category_name: The name of the category (e.g., "atom_site", "citation", etc.)
        """
        if not self.permissive:
            return
            
        # Ensure XSD is parsed in the mapping generator
        self.mapping_generator._ensure_xsd_parsed()
        
        # Get required elements from XSD schema parsing (if available)
        if hasattr(self.mapping_generator, '_xsd_complex_types') and self.mapping_generator._xsd_complex_types:
            # Query XSD for required elements for this category type
            category_required_elements = self._get_required_elements_from_xsd(category_name)
            
            for elem_name in category_required_elements:
                # Check if this element already exists
                existing_elem = row_elem.find(elem_name)
                if existing_elem is None:
                    # Add element with appropriate null indicator based on XSD type
                    safe_elem_name = self._sanitize_xml_name(elem_name)
                    if safe_elem_name:
                        null_elem = ET.SubElement(row_elem, safe_elem_name)
                        # Determine appropriate null value from XSD type information
                        null_value = self._get_appropriate_null_value_from_xsd(elem_name, category_name)
                        null_elem.text = null_value
        else:
            # If XSD parsing is not available, don't add anything
            # Let validation fail transparently to show the real data quality issues
            print(f"⚠️ No XSD schema information available for {category_name} - skipping permissive additions")
            
    def _get_required_elements_from_xsd(self, category_name: str) -> List[str]:
        """Get list of required elements for a category from XSD schema analysis.
        
        Args:
            category_name: The category name (e.g., "atom_site")
            
        Returns:
            List of element names that are required by the XSD schema
        """
        required_elements = []
        
        # Ensure XSD is parsed
        if not hasattr(self.mapping_generator, '_xsd_complex_types'):
            self.mapping_generator._ensure_xsd_parsed()
            
        # Check if we have XSD complex types available
        if not hasattr(self.mapping_generator, '_xsd_complex_types') or not self.mapping_generator._xsd_complex_types:
            return required_elements
            
        # Look for the complex type that defines this category
        category_type_name = f"{category_name}Type"
        
        # Check various naming patterns used in PDBML XSD
        type_patterns = [
            category_type_name,
            f"pdbx:{category_name}Type", 
            f"{category_name}_type",
            f"pdbx_{category_name}_type"
        ]
        
        for type_name in type_patterns:
            if type_name in self.mapping_generator._xsd_complex_types:
                complex_type_info = self.mapping_generator._xsd_complex_types[type_name]
                required_elements = complex_type_info.get('required_elements', [])
                if required_elements:
                    break
        
        return required_elements
        
    def _get_appropriate_null_value_from_xsd(self, elem_name: str, category_name: str) -> str:
        """Determine appropriate null value based on XSD element type.
        
        Args:
            elem_name: Name of the element
            category_name: Category containing the element
            
        Returns:
            Appropriate null value based on XSD type ("0" for numeric, empty for text)
        """
        # Ensure XSD is parsed
        if not hasattr(self.mapping_generator, '_xsd_complex_types'):
            self.mapping_generator._ensure_xsd_parsed()
            
        # Check if we have XSD complex types available
        if not hasattr(self.mapping_generator, '_xsd_complex_types') or not self.mapping_generator._xsd_complex_types:
            return ""  # Omit content if no XSD available
            
        # Look for the complex type that defines this category
        category_type_name = f"{category_name}Type"
        
        # Check various naming patterns used in PDBML XSD
        type_patterns = [
            category_type_name,
            f"pdbx:{category_name}Type", 
            f"{category_name}_type",
            f"pdbx_{category_name}_type"
        ]
        
        for type_name in type_patterns:
            if type_name in self.mapping_generator._xsd_complex_types:
                complex_type_info = self.mapping_generator._xsd_complex_types[type_name]
                
                # Look for the specific element within this complex type
                for elem_info in complex_type_info.get('elements', []):
                    if elem_info['name'] == elem_name:
                        element_type = elem_info.get('type', '')
                        
                        # Determine appropriate null value based on type
                        if any(numeric_type in element_type.lower() for numeric_type in 
                              ['decimal', 'float', 'double', 'int', 'integer']):
                            # For numeric types, use "0" as it's a valid numeric value
                            return "0"
                        if any(string_type in element_type.lower() for string_type in 
                                ['string', 'text', 'token', 'normalizedstring']):
                            # For string types, use empty string
                            return ""
                        # For unknown types, default to empty string
                        return ""
                break
        
        # Fallback: return empty string to omit element content
        return ""


class RelationshipResolver:
    """Resolve parent-child relationships and create nested JSON."""
    
    def __init__(self, dictionary: Optional[DictionaryParser] = None):
        """Initialize resolver with optional dictionary for relationship metadata."""
        self.dictionary = dictionary
        
    def resolve_relationships(self, xml_content: str) -> Dict[str, Any]:
        """Parse XML and resolve relationships into nested JSON."""
        try:
            # Parse XML with simple parser since recover is not supported in stdlib ElementTree
            root = ET.fromstring(xml_content)
            
            # Extract namespace
            namespace = self._extract_namespace(root)
            
            # Build category data structure
            categories = self._extract_categories(root, namespace)
            
            # Resolve relationships
            nested_data = self._build_nested_structure(categories)
            
            return nested_data
            
        except ET.ParseError:
            print("⚠️ XML parsing error, trying fallback method...")
            try:
                # Try with lxml which has better error recovery
                try:
                    root = etree.fromstring(xml_content.encode('utf-8'), parser=etree.XMLParser(recover=True))
                    # Convert lxml Element to ElementTree Element
                    root_str = etree.tostring(root)
                    root = ET.fromstring(root_str)
                    
                    # Extract namespace
                    namespace = self._extract_namespace(root)
                    
                    # Build category data structure
                    categories = self._extract_categories(root, namespace)
                    
                    # Resolve relationships
                    nested_data = self._build_nested_structure(categories)
                    
                    return nested_data
                    
                except ImportError:
                    print("⚠️ lxml not available for XML recovery, using regex fallback")
                    raise
            except Exception:
                # If all parsing fails, go to except block
                raise
        except Exception as e:
            print(f"⚠️ Error resolving relationships from XML: {str(e)}. Falling back to simple data extraction...")
            
            # Fallback to a simple structure with limited information
            try:
                # Try a simpler extraction without namespace handling
                simple_data = self._extract_simple_data(xml_content)
                return simple_data
            except Exception as e2:
                print(f"⚠️ Simple extraction also failed: {str(e2)}")
                return {"error": f"Failed to extract data: {str(e)}"}
    
    def _extract_simple_data(self, xml_content: str) -> Dict[str, Any]:
        """Fallback method for simple data extraction from potentially invalid XML."""
        data = {}
        categories_data = {}
        
        # Try to extract data without full XML parsing
        # Just look for basic patterns like <category>
        
        # Extract datablock name
        match = re.search(r'datablockName="([^"]+)"', xml_content)
        if match:
            data["datablock"] = match.group(1)
            
        # Extract category names
        categories = re.findall(r'<(\w+)Category>', xml_content)
        
        # Process each category
        for category in set(categories):
            # Build a simple regex to extract data from this category
            cat_pattern = rf'<{category}Category>(.*?)</{category}Category>'
            cat_matches = re.findall(cat_pattern, xml_content, re.DOTALL)
            
            if cat_matches:
                # Process category items (rows)
                categories_data[category] = []
                
                # Extract individual items
                item_pattern = rf'<{category}[^>]*>(.*?)</{category}>'
                for cat_content in cat_matches:
                    item_matches = re.findall(item_pattern, cat_content, re.DOTALL)
                    
                    for item_content in item_matches:
                        # Extract attributes
                        attr_pattern = r'(\w+)="([^"]*)"'
                        attrs = dict(re.findall(attr_pattern, item_content))
                        
                        if attrs:
                            categories_data[category].append(attrs)
                
                # If we couldn't extract items, at least have an empty array
                if not categories_data[category]:
                    categories_data[category] = []
        
        # Add categories data to main dictionary
        data.update(categories_data)
        
        return data
    
    def _extract_namespace(self, root: ET.Element) -> str:
        """Extract namespace from XML root element."""
        if root.tag.startswith('{'):
            return root.tag.split('}')[0][1:]
        return ""
    
    def _extract_categories(self, root: ET.Element, namespace: str) -> Dict[str, List[Dict[str, Any]]]:
        """Extract category data from XML."""
        categories = {}
        ns = f"{{{namespace}}}" if namespace else ""
        
        for category_elem in root:
            if category_elem.tag.endswith('Category'):
                category_name = category_elem.tag.replace(ns, '').replace('Category', '')
                categories[category_name] = []
                
                for item_elem in category_elem:
                    item_data = dict(item_elem.attrib)  # Start with attributes
                    
                    # Add child elements
                    for child in item_elem:
                        child_name = child.tag.replace(ns, '')
                        item_data[child_name] = child.text or ""
                    
                    categories[category_name].append(item_data)
        
        return categories
    
    def _build_nested_structure(self, categories: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Build nested JSON structure based on relationships."""
        nested = {}
        
        # First, identify parent-child relationships
        relationships = self._identify_relationships(categories)
        
        # Start with root categories (those that are not children of any other category)
        root_categories = []
        for category_name in categories:
            if category_name not in relationships.get('parents', {}):
                root_categories.append(category_name)
        
        # Build the nested structure starting from root categories
        for root_category in root_categories:
            if root_category in categories:
                nested[root_category] = self._nest_category_items(
                    root_category, categories[root_category], categories, relationships
                )
        
        return nested
    
    def _identify_relationships(self, categories: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Identify parent-child relationships in the data."""
        relationships = {
            'parents': {},  # child_category -> parent_category
            'children': {},  # parent_category -> [child_categories]
            'links': {}     # child_category -> parent_key_field
        }
        
        # Enhanced mmCIF relationship patterns based on standard _item_linked definitions
        mmcif_relationships = {
            # Core structural relationships
            'entity_poly': ('entity', 'entity_id'),
            'entity_poly_seq': ('entity_poly', 'entity_id'),  # Note: this links to entity via entity_id
            'struct_asym': ('entity', 'entity_id'),
            'atom_site': ('struct_asym', 'label_asym_id'),  # Primary link to struct_asym
            
            # Additional relationships for atom_site (multi-parent)
            'atom_site_entity': ('entity', 'label_entity_id'),
            'atom_site_seq': ('entity_poly_seq', 'label_seq_id'),
            
            # Citation relationships
            'citation_author': ('citation', 'citation_id'),
            'citation_editor': ('citation', 'citation_id'),
            
            # Chemical component relationships
            'chem_comp_atom': ('chem_comp', 'comp_id'),
            'chem_comp_bond': ('chem_comp', 'comp_id'),
            'chem_comp_angle': ('chem_comp', 'comp_id'),
            
            # Database relationships
            'database_2': ('entry', 'entry_id'),
            'pdbx_database_status': ('entry', 'entry_id'),
        }
        
        # Use dictionary relationships if available
        if self.dictionary:
            for child_category in categories:
                parents = self.dictionary.get_parent_relationships(child_category)
                for parent_info in parents:
                    parent_cat = parent_info['parent_name'].split('.')[0].lstrip('_')
                    parent_key = parent_info['parent_name'].split('.')[-1]
                    
                    if parent_cat in categories:  # Only if parent category exists in data
                        relationships['parents'][child_category] = parent_cat
                        if parent_cat not in relationships['children']:
                            relationships['children'][parent_cat] = []
                        relationships['children'][parent_cat].append(child_category)
                        relationships['links'][child_category] = parent_key
        
        # Fallback to mmCIF pattern analysis
        for child_category, (parent_category, link_field) in mmcif_relationships.items():
            # Check if both categories exist in our data
            if child_category in categories and parent_category in categories:
                # Verify the link field exists in child data
                child_items = categories[child_category]
                if child_items and any(link_field in item for item in child_items):
                    relationships['parents'][child_category] = parent_category
                    if parent_category not in relationships['children']:
                        relationships['children'][parent_category] = []
                    if child_category not in relationships['children'][parent_category]:
                        relationships['children'][parent_category].append(child_category)
                    relationships['links'][child_category] = link_field
        
        # Special handling for multi-level nesting (entity_poly_seq should nest under entity_poly)
        if 'entity_poly_seq' in categories and 'entity_poly' in categories:
            # entity_poly_seq should be nested under entity_poly, not directly under entity
            if 'entity_poly_seq' in relationships['parents']:
                # Change the parent from entity to entity_poly for proper nesting
                relationships['parents']['entity_poly_seq'] = 'entity_poly'
                relationships['links']['entity_poly_seq'] = 'entity_id'
                
                # Update children mapping
                if 'entity' in relationships['children']:
                    if 'entity_poly_seq' in relationships['children']['entity']:
                        relationships['children']['entity'].remove('entity_poly_seq')
                        
                if 'entity_poly' not in relationships['children']:
                    relationships['children']['entity_poly'] = []
                if 'entity_poly_seq' not in relationships['children']['entity_poly']:
                    relationships['children']['entity_poly'].append('entity_poly_seq')
        
        return relationships
    
    def _nest_category_items(
        self, 
        category_name: str, 
        items: List[Dict[str, Any]], 
        all_categories: Dict[str, List[Dict[str, Any]]], 
        relationships: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create nested structure for a category's items."""
        nested_items = {}        
        for item in items:
            # Use primary key as the key for this item
            item_key = self._get_item_key(item, category_name)
            nested_item = dict(item)
                        
            # Add child categories
            if category_name in relationships.get('children', {}):
                child_categories = relationships['children'][category_name]
                
                for child_category in child_categories:
                    if child_category in all_categories:
                        link_field = relationships['links'].get(child_category)
                        
                        # Find child items that link to this parent item
                        child_items = []
                        for child_item in all_categories[child_category]:
                            # Check if this child item links to current parent item
                            if link_field in child_item:
                                child_link_value = str(child_item[link_field])
                                parent_key_str = str(item_key)
                                
                                # Handle different linking scenarios
                                link_match = False
                                
                                # Direct key match
                                if child_link_value == parent_key_str:
                                    link_match = True
                                
                                # For entity relationships, also check if link value matches entity id
                                elif link_field == 'entity_id' and child_link_value == item.get('id', ''):
                                    link_match = True
                                
                                # For struct_asym relationships via label_asym_id
                                elif link_field == 'label_asym_id' and child_link_value == item.get('id', ''):
                                    link_match = True
                                
                                if link_match:
                                    # Create a deep copy of the child item
                                    nested_child_item = dict(child_item)
                                    
                                    # Recursively add grandchildren to this child
                                    if child_category in relationships.get('children', {}):
                                        grandchild_categories = relationships['children'][child_category]
                                        for grandchild_category in grandchild_categories:
                                            if grandchild_category in all_categories:
                                                grandchild_link_field = relationships['links'].get(grandchild_category)
                                                child_key = self._get_item_key(child_item, child_category)
                                                
                                                # Find grandchildren that link to this child
                                                grandchild_items = []
                                                for grandchild_item in all_categories[grandchild_category]:
                                                    if grandchild_link_field in grandchild_item:
                                                        grandchild_link_value = str(grandchild_item[grandchild_link_field])
                                                        child_key_str = str(child_key)
                                                        
                                                        # Check for link match
                                                        gc_link_match = False
                                                        if grandchild_link_value == child_key_str:
                                                            gc_link_match = True
                                                        elif grandchild_link_field == 'entity_id' and grandchild_link_value == child_item.get('id', ''):
                                                            gc_link_match = True
                                                        elif grandchild_link_field == 'entity_id' and grandchild_link_value == child_item.get('entity_id', ''):
                                                            gc_link_match = True
                                                        
                                                        if gc_link_match:
                                                            grandchild_items.append(grandchild_item)
                                                
                                                if grandchild_items:
                                                    if len(grandchild_items) == 1:
                                                        nested_child_item[grandchild_category] = grandchild_items[0]
                                                    else:
                                                        nested_child_item[grandchild_category] = grandchild_items
                                    
                                    child_items.append(nested_child_item)
                        
                        if child_items:
                            # If only one child item, nest it directly; if multiple, keep as list
                            if len(child_items) == 1:
                                # For single child, nest the object directly
                                nested_item[child_category] = child_items[0]
                            else:
                                # For multiple children, keep as array
                                nested_item[child_category] = child_items
            
            nested_items[item_key] = nested_item
        
        return nested_items
    
    def _get_item_key(self, item: Dict[str, Any], category_name: str) -> str:
        """Get the primary key for an item."""
        # Note: category_name parameter kept for future use but not currently used
        _ = category_name  # Explicitly mark as unused
        # Common key patterns
        key_fields = ['id', 'name', 'code']
        
        for field in key_fields:
            if field in item:
                return str(item[field])
        
        # Fallback to first attribute
        return str(list(item.values())[0]) if item else "unknown"


class MMCIFToPDBMLPipeline:
    """Complete pipeline for mmCIF to PDBML conversion and relationship resolution."""
    
    def __init__(self, dictionary_path: Optional[Union[str, Path]] = None, schema_path: Optional[Union[str, Path]] = None, 
                 permissive: bool = False):
        """Initialize pipeline with optional dictionary and schema paths.
        
        Args:
            dictionary_path: Path to mmCIF dictionary file
            schema_path: Path to XSD schema file  
            permissive: If True, add mmCIF null indicators for missing required schema elements
        """
        # Set default paths if not provided
        current_dir = Path(__file__).parent
        
        if dictionary_path is None:
            dictionary_path = current_dir / "schemas" / "mmcif_pdbx_v50.dic"
        if schema_path is None:
            schema_path = current_dir / "schemas" / "pdbx-v50.xsd"
        
        self.dictionary_path = Path(dictionary_path)
        self.schema_path = Path(schema_path)
        self.permissive = permissive
        
        # Initialize components
        self.dictionary = DictionaryParser()
        if self.dictionary_path.exists():
            self.dictionary.parse_dictionary(self.dictionary_path)
        
        self.converter = PDBMLConverter(
            self.dictionary_path if self.dictionary_path.exists() else None, 
            permissive=self.permissive
        )
        self.validator = XMLSchemaValidator(str(self.schema_path)) if self.schema_path.exists() else None
        self.resolver = RelationshipResolver(self.dictionary if self.dictionary_path.exists() else None)
    def process_mmcif_file(self, mmcif_path: Union[str, Path]) -> Dict[str, Any]:
        """Complete pipeline: parse mmCIF -> convert to PDBML -> validate -> resolve relationships."""
        try:
            # Step 1: Parse mmCIF
            parser = MMCIFParser(validator_factory=None)
            mmcif_container = parser.parse_file(mmcif_path)
            
            # Step 2: Convert to PDBML XML
            pdbml_xml = self.converter.convert_to_pdbml(mmcif_container)
            
            # Step 3: Validate XML against schema
            validation_results = {"is_valid": True, "errors": []}
            if self.validator:
                try:
                    validation_result = self.validator.validate(pdbml_xml)
                    is_valid = validation_result["valid"]
                    errors = validation_result.get("errors", [])
                    validation_results = {"is_valid": is_valid, "errors": errors}
                    
                    if not is_valid:
                        print(f"⚠️ Warning: XML validation failed with {len(errors)} errors:")
                        for error in errors[:5]:  # Show first 5 errors
                            print(f"  - {error}")
                except Exception as e:
                    print(f"⚠️ Warning: Error during XML validation: {str(e)}")
                    validation_results = {"is_valid": False, "errors": [str(e)]}
            
            # Step 4: Resolve relationships and create nested JSON
            nested_json = {}
            try:
                nested_json = self.resolver.resolve_relationships(pdbml_xml)
            except Exception as e:
                print(f"⚠️ Warning: Error resolving relationships: {str(e)}")
                # Continue without relationship resolution
                nested_json = {"error": str(e)}
                
                # Save the problematic XML for debugging
                with open("debug_problem_xml.xml", "w", encoding="utf-8") as f:
                    f.write(pdbml_xml)
            
            return {
                "mmcif_data": mmcif_container,
                "pdbml_xml": pdbml_xml,
                "validation": validation_results,
                "nested_json": nested_json
            }
        except Exception as e:
            print(f"⚠️ Warning: Critical error in PDBML pipeline: {str(e)}")
            return {
                "mmcif_data": None,
                "pdbml_xml": f"<!-- Error generating XML: {str(e)} -->",
                "validation": {"is_valid": False, "errors": [str(e)]},
                "nested_json": {"error": str(e)}
            }
    
    def save_outputs(self, results: Dict[str, Any], output_dir: Union[str, Path], base_name: str = "output") -> Dict[str, str]:
        """Save pipeline outputs to files."""
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True)
        
        file_paths = {}
        
        # Save PDBML XML
        xml_path = output_dir / f"{base_name}.xml"
        with open(xml_path, 'w', encoding='utf-8') as f:
            f.write(results["pdbml_xml"])
        file_paths["xml"] = str(xml_path)
        
        # Save nested JSON
        json_path = output_dir / f"{base_name}_nested.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(results["nested_json"], f, indent=2)
        file_paths["json"] = str(json_path)
        
        # Save validation report
        validation_path = output_dir / f"{base_name}_validation.txt"
        with open(validation_path, 'w', encoding='utf-8') as f:
            f.write(f"Validation Status: {'PASSED' if results['validation']['is_valid'] else 'FAILED'}\n")
            f.write(f"Number of errors: {len(results['validation']['errors'])}\n\n")
            if results['validation']['errors']:
                f.write("Errors:\n")
                for error in results['validation']['errors']:
                    f.write(f"  - {error}\n")
        file_paths["validation"] = str(validation_path)
        
        return file_paths
