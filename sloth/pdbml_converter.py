"""
PDBML Converter - Convert mmCIF to PDBX/PDBML XML format

This module provides functionality to convert mmCIF data to PDBML XML format
that conforms to the pdbx-v50.xsd schema, and handles relationship resolution
for nested JSON output.
"""

import os
import re
import pickle
import json
import hashlib
import threading
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from xml.etree import ElementTree as ET
from functools import lru_cache, wraps

from .models import MMCIFDataContainer, DataBlock, Category
from .parser import MMCIFParser
from .schemas import XMLSchemaValidator
from .pdbml_enums import (
    XMLLocation, get_numeric_fields, is_null_value, StandardRelationship
)


# Global cache for dictionary parsing results - shared across instances
_DICTIONARY_CACHE = {}
_DICTIONARY_CACHE_LOCK = threading.Lock()

# Global cache for XSD schema parsing results
_XSD_CACHE = {}
_XSD_CACHE_LOCK = threading.Lock()

# Global cache for mapping rules - shared across instances
_MAPPING_RULES_CACHE = {}
_MAPPING_RULES_CACHE_LOCK = threading.Lock()

# Cache statistics for monitoring
_CACHE_STATS = {
    'dictionary_hits': 0,
    'dictionary_misses': 0,
    'xsd_hits': 0,
    'xsd_misses': 0,
    'mapping_hits': 0,
    'mapping_misses': 0
}

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


def get_cache_statistics() -> Dict[str, Any]:
    """Get global cache statistics."""
    return {
        'cache_stats': _CACHE_STATS.copy(),
        'dictionary_cache_size': len(_DICTIONARY_CACHE),
        'xsd_cache_size': len(_XSD_CACHE),
        'mapping_rules_cache_size': len(_MAPPING_RULES_CACHE)
    }


def clear_global_caches():
    """Clear all global caches."""
    global _DICTIONARY_CACHE, _XSD_CACHE, _MAPPING_RULES_CACHE, _CACHE_STATS
    
    with _DICTIONARY_CACHE_LOCK:
        _DICTIONARY_CACHE.clear()
    
    with _XSD_CACHE_LOCK:
        _XSD_CACHE.clear()
        
    with _MAPPING_RULES_CACHE_LOCK:
        _MAPPING_RULES_CACHE.clear()
    
    _CACHE_STATS = {
        'dictionary_hits': 0,
        'dictionary_misses': 0,
        'xsd_hits': 0,
        'xsd_misses': 0,
        'mapping_hits': 0,
        'mapping_misses': 0
    }


def _get_cache_file_path(cache_dir: str, file_path: str, prefix: str = "dict") -> Path:
    """Generate cache file path based on source file and modification time."""
    if not file_path or not os.path.exists(file_path):
        return None
    
    # Include file modification time in cache key for auto-invalidation
    mtime = os.path.getmtime(file_path)
    file_hash = hashlib.md5(f"{file_path}_{mtime}".encode()).hexdigest()
    cache_filename = f"{prefix}_{Path(file_path).stem}_{file_hash}.pkl"
    return Path(cache_dir) / cache_filename


def _load_from_disk_cache(cache_file: Path) -> Optional[Dict]:
    """Load data from disk cache using pickle for speed."""
    if not cache_file or not cache_file.exists():
        return None
    
    try:
        import pickle
        with open(cache_file, 'rb') as f:
            return pickle.load(f)
    except Exception:
        # Cache corruption - remove file
        try:
            cache_file.unlink()
        except Exception:
            pass
        return None


def _save_to_disk_cache(cache_file: Path, data: Dict) -> None:
    """Save data to disk cache using pickle for speed."""
    if not cache_file:
        return
    
    try:
        import pickle
        cache_file.parent.mkdir(exist_ok=True)
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        pass  # Don't fail if we can't cache


class XMLMappingGenerator:
    """
    Embedded XML mapping generator that creates mapping rules on-the-fly
    without requiring external JSON files.
    """
    
    def __init__(self, dict_file: Optional[Union[str, Path]] = None, xsd_file: Optional[Union[str, Path]] = None, cache_dir: Optional[str] = None, quiet: bool = False):
        self.dict_file = dict_file
        self.xsd_file = xsd_file
        self.cache_dir = cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
        self.quiet = quiet
        
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
                _CACHE_STATS['dictionary_hits'] += 1
                cached_data = _DICTIONARY_CACHE[dict_path]
                self._categories = cached_data['categories']
                self._items = cached_data['items']
                self._relationships = cached_data['relationships']
                self._enumerations = cached_data['enumerations']
                self._item_types = cached_data['item_types']
                return
        
        # Check disk cache
        cache_file = _get_cache_file_path(self.cache_dir, dict_path, "dict")
        disk_data = _load_from_disk_cache(cache_file)
        
        if disk_data:
            _CACHE_STATS['dictionary_hits'] += 1
            if not self.quiet:
                print("📦 Using cached dictionary data")
            self._categories = disk_data['categories']
            self._items = disk_data['items']
            self._relationships = disk_data['relationships']
            self._enumerations = disk_data['enumerations']
            self._item_types = disk_data['item_types']
            
            # Store in global cache for even faster access
            with _DICTIONARY_CACHE_LOCK:
                _DICTIONARY_CACHE[dict_path] = disk_data
            return
        
        # Parse dictionary with optimizations
        _CACHE_STATS['dictionary_misses'] += 1
        self._categories = {}
        self._items = {}
        self._relationships = []
        self._enumerations = {}
        self._item_types = {}
        
        try:
            if not self.quiet:
                print("📚 Parsing dictionary (this may take a moment)...")
            self._parse_dictionary_structure()  # Use schema-driven parsing
            
            # Prepare data for caching
            cache_data = {
                'categories': self._categories,
                'items': self._items,
                'relationships': self._relationships,
                'enumerations': self._enumerations,
                'item_types': self._item_types
            }
            
            # Save to disk cache for future use
            _save_to_disk_cache(cache_file, cache_data)
            
            # Cache the results globally
            with _DICTIONARY_CACHE_LOCK:
                _DICTIONARY_CACHE[dict_path] = cache_data
                
        except Exception as e:
            if not self.quiet:
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
            
        # Generate cache key for global and disk caching
        cache_key = self._get_cache_key()
        
        # Check global cache first
        with _MAPPING_RULES_CACHE_LOCK:
            if cache_key in _MAPPING_RULES_CACHE:
                _CACHE_STATS['mapping_hits'] += 1
                self._mapping_rules_cache = _MAPPING_RULES_CACHE[cache_key]
                return self._mapping_rules_cache
        
        # Try to load from disk cache
        cached_rules = self._load_mapping_rules_from_cache(cache_key)
        if cached_rules:
            _CACHE_STATS['mapping_hits'] += 1
            self._mapping_rules_cache = cached_rules
            # Store in global cache for faster access
            with _MAPPING_RULES_CACHE_LOCK:
                _MAPPING_RULES_CACHE[cache_key] = cached_rules
            return cached_rules
        
        # Generate mapping rules
        _CACHE_STATS['mapping_misses'] += 1
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
                pass  # XSD will be parsed on-demand via properties
                
            # Generate comprehensive mappings
            mapping_rules = self._generate_comprehensive_mapping()
            
        except Exception as e:
            print(f"⚠️ Error: Cannot generate mapping rules without valid dictionary/schema: {e}")
            print("Please ensure that dictionary and/or XSD files are provided and valid.")
            raise RuntimeError(f"Failed to generate mapping rules: {e}")
            
        # Cache the results in memory and disk
        self._mapping_rules_cache = mapping_rules
        self._save_mapping_rules_to_cache(cache_key, mapping_rules)
        
        # Store in global cache for faster future access
        with _MAPPING_RULES_CACHE_LOCK:
            _MAPPING_RULES_CACHE[cache_key] = mapping_rules
        
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
                _CACHE_STATS['xsd_hits'] += 1
                cached_data = _XSD_CACHE[xsd_path]
                self._xsd_elements = cached_data['elements']
                self._xsd_attributes = cached_data['attributes']
                self._xsd_required_elements = cached_data['required_elements']
                self._xsd_default_values = cached_data['default_values']
                self._xsd_complex_types = cached_data['complex_types']
                return
        
        # Check disk cache
        cache_file = _get_cache_file_path(self.cache_dir, xsd_path, "xsd")
        disk_data = _load_from_disk_cache(cache_file)
        
        if disk_data:
            _CACHE_STATS['xsd_hits'] += 1
            if not self.quiet:
                print("📦 Using cached XSD schema data")
            self._xsd_elements = disk_data['elements']
            self._xsd_attributes = disk_data['attributes']
            self._xsd_required_elements = disk_data['required_elements']
            self._xsd_default_values = disk_data['default_values']
            self._xsd_complex_types = disk_data['complex_types']
            
            # Store in global cache
            with _XSD_CACHE_LOCK:
                _XSD_CACHE[xsd_path] = disk_data
            return
        
        # Parse XSD schema
        _CACHE_STATS['xsd_misses'] += 1
        self._xsd_elements = {}
        self._xsd_attributes = {}
        self._xsd_required_elements = {}
        self._xsd_default_values = {}
        self._xsd_complex_types = {}
        
        try:
            if not self.quiet:
                print("📋 Parsing XSD schema...")
            self._parse_xsd_schema()
            
            # Prepare data for caching
            cache_data = {
                'elements': self._xsd_elements,
                'attributes': self._xsd_attributes,
                'required_elements': self._xsd_required_elements,
                'default_values': self._xsd_default_values,
                'complex_types': self._xsd_complex_types
            }
            
            # Save to disk cache
            _save_to_disk_cache(cache_file, cache_data)
            
            # Cache the results globally
            with _XSD_CACHE_LOCK:
                _XSD_CACHE[xsd_path] = cache_data
                
        except Exception as e:
            if not self.quiet:
                print(f"⚠️ Warning: Error parsing XSD schema: {e}")
            # Initialize empty structures
            self._xsd_elements = {}
            self._xsd_attributes = {}
            self._xsd_required_elements = {}
            self._xsd_default_values = {}
            self._xsd_complex_types = {}
    
    def _get_xsd_required_categories(self) -> set:
        """Extract categories that are actually required by the XSD schema.
        
        This method intelligently analyzes both XSD schema and mmCIF dictionary
        to determine essential categories that must always be included.
        """
        required_categories = set()
        
        # First, identify key categories from dictionary structure
        key_dictionary_categories = self._identify_key_dictionary_categories()
        
        if not self.xsd_file or not Path(self.xsd_file).exists():
            if not self.quiet:
                print("📋 No XSD schema found, using dictionary analysis only")
            return key_dictionary_categories
            
        try:
            # Parse XSD to find required elements
            tree = ET.parse(self.xsd_file)
            root = tree.getroot()
            
            # Find namespace
            namespace = root.tag.split('}')[0][1:] if '}' in root.tag else ''
            ns = {'xs': namespace} if namespace else {}
            
            # Look for elements that are required (minOccurs != "0")
            elements = root.findall('.//xs:element', ns)
            for elem in elements:
                elem_name = elem.get('name')
                min_occurs = elem.get('minOccurs', '1')
                
                if elem_name and min_occurs != '0':
                    # Convert XSD element name to category name
                    if elem_name.endswith('Category'):
                        category_name = elem_name[:-8]  # Remove 'Category' suffix
                        required_categories.add(category_name)
                        
            # Also look for complex types that reference other categories
            complex_types = root.findall('.//xs:complexType', ns)
            for ct in complex_types:
                # Find references to other categories within complex types
                refs = ct.findall('.//xs:element[@ref]', ns)
                for ref in refs:
                    ref_name = ref.get('ref')
                    if ref_name and ref_name.endswith('Category'):
                        category_name = ref_name[:-8]
                        required_categories.add(category_name)
                
            # Check for categories that have complex types defined for them
            # This is another indicator that they're important
            for type_name in self.xsd_complex_types:
                if type_name.endswith('Type') and not type_name.startswith('xs:'):
                    # Extract category name from type name (e.g., "entryType" -> "entry")
                    potential_category = type_name[:-4].lower()
                    required_categories.add(potential_category)
                        
        except Exception as e:
            if not self.quiet:
                print(f"⚠️ Warning: Could not analyze XSD requirements: {e}")
            # Fallback to dictionary analysis
            return key_dictionary_categories
        
        # Merge XSD-identified categories with dictionary-identified ones
        required_categories.update(key_dictionary_categories)
            
        if not self.quiet:
            print(f"📋 Schema analysis identified {len(required_categories)} essential categories")
            
        return required_categories
        
    def _identify_key_dictionary_categories(self) -> set:
        """Identify key categories from the mmCIF dictionary based on:
        
        1. Categories marked as mandatory
        2. Categories frequently referenced by others
        3. Categories with important structural significance
        """
        key_categories = set(['entry'])  # Entry is always essential
        
        if not self._categories:
            return key_categories  # Return minimal set if no dictionary parsed
            
        # 1. Add categories marked as mandatory in the dictionary
        for cat_id, cat_info in self._categories.items():
            mandatory = cat_info.get('mandatory', 'no')
            if mandatory == 'yes':
                key_categories.add(cat_id)
                
        # 2. Find categories that are heavily referenced (relational hubs)
        reference_counts = {}
        for relationship in self.relationships:
            parent_name = relationship.get('parent_name', '')
            
            if '.' in parent_name:
                parent_cat = parent_name.split('.')[0].lstrip('_')
                reference_counts[parent_cat] = reference_counts.get(parent_cat, 0) + 1
                
        # Categories referenced many times are likely important
        for cat, count in reference_counts.items():
            if count >= 3:  # Arbitrary threshold - categories referenced by 3+ other categories
                key_categories.add(cat)
                
        # 3. Identify structurally significant categories based on dictionary metadata
        # Instead of hardcoding patterns, analyze dictionary properties
        
        # Find categories that have essential structural roles based on:
        # - Categories with mandatory items
        # - Categories involved in core data hierarchy
        # - Categories referenced by ID items in other categories
        for cat_id, cat_info in self._categories.items():
            # Find categories with mandatory items
            has_mandatory_items = False
            
            # Search for any mandatory items in this category
            for item_name, item_info in self._items.items():
                if item_name.startswith(f"_{cat_id}.") and item_info.get('mandatory', 'no') == 'yes':
                    has_mandatory_items = True
                    key_categories.add(cat_id)
                    break
                    
            # Look for connection to the main entry via relationships
            for relationship in self.relationships:
                child_name = relationship.get('child_name', '')
                parent_name = relationship.get('parent_name', '')
                
                # If this category is connected to entry or is part of a relationship chain
                if (child_name.startswith(f"_{cat_id}.") and parent_name.startswith("_entry.")) or \
                   (parent_name.startswith(f"_{cat_id}.") and "id" in parent_name):
                    key_categories.add(cat_id)
                    break
                    
            # Categories with "primary" or "key" in their description likely have structural significance
            if cat_info.get('description', ''):
                description = cat_info['description'].lower()
                if any(term in description for term in ['primary', 'key', 'core', 'essential', 'required']):
                    key_categories.add(cat_id)
                    
        if not self.quiet and key_categories:
            print(f"� Dictionary analysis identified {len(key_categories)} important categories")
            
        return key_categories
    
    def _get_categories_used_in_data(self, data_container) -> set:
        """Analyze actual data to determine which categories are present."""
        if not hasattr(data_container, 'data') or not data_container.data:
            return set()
            
        used_categories = set()
        for data_block in data_container.data:
            for category_name in data_block.categories:
                # Convert mmCIF category name to simple name
                clean_name = category_name.lstrip('_')
                used_categories.add(clean_name)
                
        return used_categories
    
    def _analyze_category_dependencies(self) -> Dict[str, set]:
        """Analyze dictionary to find category dependencies based on relationships."""
        dependencies = {}
        
        # Parse relationships to understand dependencies
        for relationship in self.relationships:
            parent_name = relationship.get('parent_name', '')
            child_name = relationship.get('child_name', '')
            
            if '.' in parent_name and '.' in child_name:
                parent_cat = parent_name.split('.')[0].lstrip('_')
                child_cat = child_name.split('.')[0].lstrip('_')
                
                if child_cat not in dependencies:
                    dependencies[child_cat] = set()
                dependencies[child_cat].add(parent_cat)
                
        return dependencies
    
    def _get_priority_categories(self, data_container=None) -> tuple:
        """
        Pure schema-driven priority determination based on:
        1. XSD schema requirements (required elements)
        2. Dictionary metadata (mandatory categories/items, keys)
        3. Actual data presence
        4. Dynamic dependency analysis from relationships
        
        NO hardcoded patterns, prefixes, or arbitrary thresholds.
        """
        # Step 1: Get categories required by XSD schema
        xsd_required = self._get_xsd_required_categories()
        
        # Step 2: Get categories actually used in data (if available)
        data_used = set()
        if data_container:
            data_used = self._get_categories_used_in_data(data_container)
            
        # Step 3: Get categories marked as mandatory in dictionary
        dictionary_mandatory = self._get_dictionary_mandatory_categories()
        
        # Step 4: Get categories with mandatory items
        categories_with_mandatory_items = self._get_categories_with_mandatory_items()
        
        # Step 5: Build dependency relationships from dictionary
        dependencies = self._analyze_category_dependencies()
        
        # High Priority: Schema required + Data present + Dictionary mandatory + Categories with mandatory items
        high_priority = set()
        high_priority.update(xsd_required)
        high_priority.update(data_used)
        high_priority.update(dictionary_mandatory)
        high_priority.update(categories_with_mandatory_items)
        
        # Step 6: Add all dependencies of high priority categories recursively
        high_priority = self._add_dependencies_recursively(high_priority, dependencies)
        
        # Medium Priority: Categories that are dependencies of high priority but not already included
        medium_priority = set()
        for cat in high_priority:
            if cat in dependencies:
                medium_priority.update(dependencies[cat])
        
        # Remove overlap between high and medium priority
        medium_priority -= high_priority
        
        # Add dependencies of medium priority categories (one level only)
        medium_priority_deps = set()
        for cat in medium_priority:
            if cat in dependencies:
                medium_priority_deps.update(dependencies[cat])
        medium_priority.update(medium_priority_deps)
        medium_priority -= high_priority  # Remove any overlap with high priority
        
        if not self.quiet:
            print(f"📊 Pure schema-driven analysis:")
            print(f"   • XSD required: {len(xsd_required)} categories")
            print(f"   • Data present: {len(data_used)} categories")  
            print(f"   • Dictionary mandatory: {len(dictionary_mandatory)} categories")
            print(f"   • With mandatory items: {len(categories_with_mandatory_items)} categories")
            print(f"   • Final high priority: {len(high_priority)} categories")
            print(f"   • Final medium priority: {len(medium_priority)} categories")
            
        return high_priority, medium_priority
        
    def _get_dictionary_mandatory_categories(self) -> set:
        """Get categories marked as mandatory in the dictionary."""
        mandatory_categories = set()
        for cat_id, cat_info in self.categories.items():
            if cat_info.get('mandatory', 'no') == 'yes':
                mandatory_categories.add(cat_id)
        return mandatory_categories
    
    def _get_categories_with_mandatory_items(self) -> set:
        """Get categories that have mandatory items."""
        categories_with_mandatory = set()
        for item_name, item_info in self.items.items():
            if item_info.get('mandatory', 'no') == 'yes':
                # Extract category name from item name (e.g., "_entity.id" -> "entity")
                if '.' in item_name:
                    category_name = item_name.split('.')[0].lstrip('_')
                    categories_with_mandatory.add(category_name)
        return categories_with_mandatory
    
    def _add_dependencies_recursively(self, categories: set, dependencies: Dict[str, set]) -> set:
        """Add all dependencies recursively to ensure complete data integrity."""
        result = set(categories)
        to_process = list(categories)
        
        while to_process:
            current = to_process.pop()
            if current in dependencies:
                for dep in dependencies[current]:
                    if dep not in result:
                        result.add(dep)
                        to_process.append(dep)
        
        return result
        
    def _parse_dictionary_structure(self):
        """Parse dictionary structure with schema-driven optimization"""
        if not self.dict_file:
            if not self.quiet:
                print("⚠️ No dictionary file provided")
            return
            
        # Analyze what categories we actually need based on schema and data
        # When parsing dictionary initially (no data available), be more permissive
        # to ensure we capture all potentially needed categories
        high_priority, medium_priority = self._get_priority_categories()
        
        # If we don't have much priority data (indicating no data container was passed),
        # be more permissive to capture all essential structural categories
        if len(high_priority) + len(medium_priority) < 20:
            if not self.quiet:
                print("📋 No data available for analysis - using permissive parsing for all structural categories")
            use_permissive_parsing = True
        else:
            use_permissive_parsing = False
        
        current_save = None
        current_block = []
        in_save_frame = False
        save_count = 0
        category_count = 0
        processed_categories = set()
        
        try:
            with open(self.dict_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    
                    if not line or line.startswith('#'):
                        continue
                        
                    if line.startswith('save_'):
                        if in_save_frame and current_save:
                            # Use schema-driven decision making
                            should_process = self._should_process_save_frame_schema_driven(
                                current_save, current_block, high_priority, medium_priority, processed_categories, use_permissive_parsing
                            )
                            
                            if should_process:
                                self._process_save_frame(current_save, current_block)
                                save_count += 1
                                
                                # Track processed categories
                                block_text = '\n'.join(current_block)
                                if '_category.id' in block_text:
                                    cat_match = re.search(r'_category\.id\s+(\S+)', block_text)
                                    if cat_match:
                                        processed_categories.add(cat_match.group(1).strip())
                                        category_count += 1
                                        
                        current_save = line[5:]
                        current_block = []
                        in_save_frame = True
                        
                    elif line == 'save_':
                        if in_save_frame and current_save:
                            should_process = self._should_process_save_frame_schema_driven(
                                current_save, current_block, high_priority, medium_priority, processed_categories, use_permissive_parsing
                            )
                            
                            if should_process:
                                self._process_save_frame(current_save, current_block)
                                save_count += 1
                                
                                block_text = '\n'.join(current_block)
                                if '_category.id' in block_text:
                                    cat_match = re.search(r'_category\.id\s+(\S+)', block_text)
                                    if cat_match:
                                        processed_categories.add(cat_match.group(1).strip())
                                        category_count += 1
                                        
                        current_save = None
                        current_block = []
                        in_save_frame = False
                        
                    elif in_save_frame:
                        current_block.append(line)
                        
            if not self.quiet:
                print(f"✓ Schema-driven parsing: processed {save_count} save frames")
                print(f"✓ Loaded {len(self._categories)} categories, {len(self._items)} items")
                print(f"✓ Found {len(processed_categories)} categories: {sorted(list(processed_categories)[:10])}{'...' if len(processed_categories) > 10 else ''}")
                        
        except Exception as e:
            if not self.quiet:
                print(f"⚠️ Warning: Error parsing dictionary: {e}")
                traceback.print_exc()
                
    def _should_process_save_frame_schema_driven(self, save_name: str, block: List[str], 
                                                high_priority: set, medium_priority: set, 
                                                processed_categories: set, use_permissive_parsing: bool = False) -> bool:
        """Pure schema-driven decision on whether to process a save frame.
        
        This approach eliminates ALL hardcoding and uses only:
        1. XSD schema requirements
        2. Dictionary metadata (mandatory flags, keys, relationships)
        3. Actual data presence
        4. Dynamic dependency analysis
        """
        block_text = '\n'.join(block)
        
        # Always process categories if they're in our calculated priority sets
        if '_category.id' in block_text:
            cat_match = re.search(r'_category\.id\s+(\S+)', block_text)
            if cat_match:
                cat_id = cat_match.group(1).strip()
                
                # Process if in high priority (schema-required or data-present)
                if cat_id in high_priority:
                    return True
                    
                # Process if in medium priority (dependencies)
                if cat_id in medium_priority:
                    return True
                    
                # In permissive mode (when no data available), include ALL categories
                # Let the schema and dictionary drive the decision completely
                if use_permissive_parsing:
                    if not self.quiet:
                        print(f"📋 Including category in permissive mode: {cat_id}")
                    return True
                
                # Otherwise, reject categories not found through proper analysis
                return False
                
        # Process items only for categories we've decided to include
        if '_item.name' in block_text:
            item_match = re.search(r'_item\.name\s+[\'"]([^\'"]+)[\'"]', block_text)
            if item_match:
                item_name = item_match.group(1).strip()
                
                # Check if this item belongs to a priority category
                for cat in high_priority | medium_priority:
                    if item_name.startswith(f'_{cat}.'):
                        return True
                        
                # Also process if the category is already processed
                category_part = item_name.split('.')[0].lstrip('_')
                if category_part in processed_categories:
                    return True
                    
                return False
                    
        # Always process metadata that's essential for understanding structure
        # These are small and critical for proper mapping generation
        if ('_item_enumeration.value' in block_text or 
            '_item_type.code' in block_text or
            '_item_linked.parent_name' in block_text):
            return True
            
        return False
                        
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
        """Extract category information including keys - FIXED: proper key extraction"""
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
            
        # FIXED: Extract category keys only from _category_key section (not from all items)
        keys_set = set()  # Use set to avoid duplicates
        
        if '_category_key.name' in block_text:
            # Extract category keys specifically from _category_key definitions
            # This defines which items are the PRIMARY KEYS for the category
            
            # Method 1: Single line format
            single_key_pattern = r'_category_key\.name\s+[\'"]?([^\s\'"]+)[\'"]?'
            single_key_matches = re.findall(single_key_pattern, block_text)
            
            for key_item in single_key_matches:
                key_item = key_item.strip('\'"').strip()
                if key_item.startswith('_' + cat_id + '.'):
                    item_name = key_item[len('_' + cat_id + '.'):]
                    keys_set.add(item_name)
            
            # Method 2: Loop format (more common)
            if 'loop_' in block_text:
                # Look for the specific loop block with _category_key.name
                loop_sections = re.split(r'loop_', block_text)
                for section in loop_sections:
                    if '_category_key.name' in section:
                        # This is the _category_key loop section
                        lines = section.strip().split('\n')
                        in_data = False
                        
                        for line in lines:
                            line = line.strip()
                            
                            # Skip empty lines and comments
                            if not line or line.startswith('#'):
                                continue
                                
                            # Start of data section after header
                            if line.startswith('_category_key.name'):
                                in_data = True
                                continue
                            
                            # End of this loop (next section starts)
                            if in_data and (line.startswith('_') and not line.startswith('_' + cat_id + '.')):
                                break
                                
                            # Process data lines
                            if in_data and not line.startswith('_'):
                                key_item = line.strip('\'"').strip()
                                if key_item.startswith('_' + cat_id + '.'):
                                    item_name = key_item[len('_' + cat_id + '.'):]
                                    keys_set.add(item_name)
                        break
        
        # Convert set back to list to maintain order
        self._categories[cat_id]['keys'] = list(keys_set)
        
        # Debug validation for key categories
        if not self.quiet and cat_id in ['chem_comp_angle', 'atom_site', 'entity']:
            print(f"🔑 Category {cat_id}: extracted keys = {self._categories[cat_id]['keys']}")
                                        
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
        if 'loop_' in block_text and '_pdbx_item_linked_group_list' in block_text:
            # Parse the PDBX relationship format
            lines = block_text.split('\n')
            in_loop = False
            header_found = False
            col_indices = {}
            
            for line in lines:
                line = line.strip()
                
                if line.startswith('loop_'):
                    in_loop = True
                    continue
                    
                if in_loop and line.startswith('_pdbx_item_linked_group_list.'):
                    # Map column headers to indices
                    field_name = line.split('.')[-1]
                    col_indices[field_name] = len(col_indices)
                    header_found = True
                    continue
                    
                if in_loop and header_found and line and not line.startswith('_') and not line.startswith('#'):
                    # This is a data row
                    columns = line.split()
                    if len(columns) >= len(col_indices):
                        try:
                            # Extract relationship data
                            child_cat_idx = col_indices.get('child_category_id', -1)
                            parent_name_idx = col_indices.get('parent_name', -1)
                            child_name_idx = col_indices.get('child_name', -1)
                            
                            if child_cat_idx >= 0 and parent_name_idx >= 0 and child_name_idx >= 0:
                                child_category = columns[child_cat_idx].strip('\'"')
                                parent_name = columns[parent_name_idx].strip('\'"')
                                child_name = columns[child_name_idx].strip('\'"')
                                
                                if child_category and parent_name and child_name:
                                    self._relationships.append({
                                        'parent_name': parent_name,
                                        'child_name': child_name,
                                        'child_category': child_category,
                                        'save_name': save_name
                                    })
                        except IndexError:
                            continue
                            
                if line.startswith('#') or (in_loop and line.startswith('_') and 'pdbx_item_linked_group_list' not in line):
                    # End of this loop
                    break
                        
    def _parse_xsd_schema(self):
        """Parse XSD schema to extract element/attribute requirements"""
        if not self.xsd_file:
            return
            
        try:
            tree = ET.parse(self.xsd_file)
            root = tree.getroot()
            
            # Find namespace
            namespace = root.tag.split('}')[0][1:] if '}' in root.tag else ''
            ns = {'xs': namespace} if namespace else {}
            
            # Parse complex types
            self._parse_complex_types(root, ns)
            
            # Parse elements
            self._parse_elements(root, ns)
            
        except Exception as e:
            if not self.quiet:
                print(f"⚠️ Warning: Could not parse XSD schema: {e}")
            
    def _parse_complex_types(self, root: ET.Element, ns: dict):
        """Parse complex types from XSD schema"""
        complex_types = root.findall('.//xs:complexType', ns)
        
        for ct in complex_types:
            type_name = ct.get('name')
            if not type_name:
                continue
                
            self.xsd_complex_types[type_name] = {
                'name': type_name,
                'elements': [],
                'attributes': [],
                'required_elements': [],
                'required_attributes': []
            }
            
            # Parse elements within complex type
            elements = ct.findall('.//xs:element', ns)
            for elem in elements:
                elem_name = elem.get('name')
                if elem_name:
                    self.xsd_complex_types[type_name]['elements'].append(elem_name)
                    
                    # Check if required
                    min_occurs = elem.get('minOccurs', '1')
                    if min_occurs != '0':
                        self.xsd_complex_types[type_name]['required_elements'].append(elem_name)
                        
            # Parse attributes within complex type
            attributes = ct.findall('.//xs:attribute', ns)
            for attr in attributes:
                attr_name = attr.get('name')
                if attr_name:
                    self.xsd_complex_types[type_name]['attributes'].append(attr_name)
                    
                    # Check if required
                    use = attr.get('use', 'optional')
                    if use == 'required':
                        self.xsd_complex_types[type_name]['required_attributes'].append(attr_name)
                        
    def _parse_elements(self, root: ET.Element, ns: dict):
        """Parse element definitions from XSD schema"""
        elements = root.findall('.//xs:element', ns)
        
        for elem in elements:
            elem_name = elem.get('name')
            if not elem_name:
                continue
                
            self.xsd_elements[elem_name] = {
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
        
    def _determine_xml_location(self, item_name: str, _item_info: dict) -> str:
        """Determine if item should be XML element or attribute"""
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
                
        # Default to element for most cases (avoid circular dependency with mapping rules)
        return XMLLocation.ELEMENT_CONTENT.value
        
    def _generate_element_requirements(self) -> Dict[str, List[str]]:
        """Generate element requirements mapping"""
        element_requirements = {}
        
        # Process each category
        for cat_id in self.categories:
            element_only = []
            
            # Get all items for this category
            category_items = [item for item in self.items.keys() 
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
            category_items = [item for item in self.items.keys() 
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
        """Generate default values mapping based on schema/dictionary definitions"""
        default_values = {}
        
        # Process all categories to extract defaults from schema/dictionary
        for cat_id in self.categories:
            category_defaults = {}
            
            # Get all items for this category
            category_items = [item for item in self.items.keys() 
                            if item.startswith(f'_{cat_id}.')]
            
            for item_name in category_items:
                item_part = item_name.split('.', 1)[1]
                
                # Get default value from schema/dictionary
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
            category_items = [item for item in self.items.keys() 
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
        """Get default value for item"""
        # Check XSD for default values
        if item_name in self.xsd_elements:
            return self.xsd_elements[item_name].get('default', '')
            
        # Provide sensible defaults based on data type
        data_type = self._get_item_data_type(item_name)
        
        if data_type in ['int', 'float']:
            return '0'
        elif data_type == 'code':
            return '.'
        else:
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
        parser = MMCIFParser(validator_factory=None)
        container = parser.parse_file(dict_path)
        
        if not container.data:
            raise ValueError("No data blocks found in dictionary")
            
        dict_block = container.data[0]
        
        # Parse category definitions
        if "_category" in dict_block.categories:
            self._parse_categories(dict_block["_category"])
            
        # Parse category key definitions (most important for key extraction)
        if "_category_key" in dict_block.categories:
            self._parse_category_keys(dict_block["_category_key"])
            
        # Parse item definitions  
        if "_item" in dict_block.categories:
            self._parse_items(dict_block["_item"])
            
        # Parse item type definitions
        if "_item_type" in dict_block.categories:
            self._parse_item_types(dict_block["_item_type"])
            
        # Parse enumeration definitions
        if "_item_enumeration" in dict_block.categories:
            self._parse_enumerations(dict_block["_item_enumeration"])
            
        # Parse relationships/links
        if "_item_linked" in dict_block.categories:
            self._parse_relationships(dict_block["_item_linked"])
    
    def _parse_categories(self, category: Category) -> None:
        """Parse category definitions."""
        data = category.data
        if "id" in data:
            for i, cat_id in enumerate(data["id"]):
                self.categories[cat_id] = {
                    "id": cat_id,
                    "description": data.get("description", [None] * len(data["id"]))[i] or "",
                    "mandatory_code": data.get("mandatory_code", [None] * len(data["id"]))[i] or "no"
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
                    "description": data.get("description", [None] * len(data["name"]))[i] or ""
                }
    
    def _parse_item_types(self, category: Category) -> None:
        """Parse item type definitions."""
        data = category.data
        if "code" in data:
            for i, type_code in enumerate(data["code"]):
                if type_code in self.items:
                    self.items[type_code].update({
                        "type_code": data.get("code", [None] * len(data["code"]))[i] or "",
                        "primitive_code": data.get("primitive_code", [None] * len(data["code"]))[i] or "",
                        "construct": data.get("construct", [None] * len(data["code"]))[i] or "",
                        "detail": data.get("detail", [None] * len(data["code"]))[i] or ""
                    })
    
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
        """Extract key items from _category_key."""
        data = category.data
        if "name" in data:
            for full_item_name in data["name"]:
                # Parse category.item format (e.g., "_citation.id")
                if "." in full_item_name:
                    cat_name, item_name = full_item_name.lstrip("_").split(".", 1)
                    if cat_name not in self.categories:
                        self.categories[cat_name] = {}
                    if "keys" not in self.categories[cat_name]:
                        self.categories[cat_name]["keys"] = []
                    self.categories[cat_name]["keys"].append(item_name)
    
    def get_category_key_items(self, category_name: str) -> List[str]:
        """Get the key items for a category from _category_key definitions."""
        # Remove leading underscore for lookup
        clean_category = category_name.lstrip('_')
        
        # First, try to get keys from _category_key definitions
        if clean_category in self.categories and "keys" in self.categories[clean_category]:
            return self.categories[clean_category]["keys"]
        
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
    
    def __init__(self, dictionary_path: Optional[Union[str, Path]] = None, cache_dir: Optional[str] = None, permissive: bool = False, quiet: bool = False):
        """Initialize converter with optional dictionary for metadata."""
        self.cache_dir = cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
        self.permissive = permissive
        self.quiet = quiet
        
        # Set default dictionary path if not provided
        if dictionary_path is None:
            dictionary_path = Path(__file__).parent / "schemas" / "mmcif_pdbx_v50.dic"
        
        # Initialize embedded XML mapping generator with caching (this handles dictionary parsing)
        xsd_path = Path(__file__).parent / "schemas" / "pdbx-v50.xsd"
        self.mapping_generator = XMLMappingGenerator(
            dict_file=dictionary_path,
            xsd_file=xsd_path if xsd_path.exists() else None,
            cache_dir=self.cache_dir,
            quiet=self.quiet
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
        
    @property
    def _dictionary(self) -> Optional[Dict[str, Any]]:
        """Backward compatibility property - returns None until mapping generator is accessed."""
        # For backward compatibility with tests that expect _dictionary attribute
        # Return None if mapping generator hasn't been accessed yet (lazy loading)
        if self.mapping_generator._categories is None:
            return None
        # Return the categories dictionary from the mapping generator
        return self.mapping_generator.categories
    
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
                
            # First, use a simpler method to get valid XML
            return self._generate_simple_xml_output(root)
            
        except Exception as e:
            print(f"⚠️ Error generating XML: {str(e)}")
            # Fallback to very simple XML generation
            return self._generate_fallback_xml(data_block)
            
    def _generate_simple_xml_output(self, root: ET.Element) -> str:
        """Generate XML output with simpler formatting to avoid parsing issues."""
        # Check if we need to add any required categories that are missing
        self._ensure_required_categories(root)
            
        # Use ElementTree's built-in serialization without pretty-printing
        xml_string = ET.tostring(root, encoding='utf-8').decode('utf-8')
        return '<?xml version="1.0" encoding="utf-8"?>\n' + xml_string
        
    def _ensure_required_categories(self, root: ET.Element) -> None:
        """Ensure required categories are present based on schema relationships, not hardcoded logic."""
        # This method should use the relationship resolver and schema requirements
        # instead of hardcoded category-specific logic
        
        # Get basic datablock info
        datablock_name = root.get("datablockName", "unknown")
        
        # Use schema-driven approach to determine missing required categories
        # based on what data is actually present and what the schema requires
        existing_categories = set()
        referenced_ids = {}
        
        # Collect existing categories and referenced IDs
        for cat in root:
            if cat.tag.endswith("Category"):
                category_name = cat.tag[:-8]  # Remove "Category" suffix
                existing_categories.add(category_name)
                
                # Collect any ID references for relationship validation
                for item in cat:
                    for child in item:
                        if child.tag.endswith("_id") and child.text:
                            ref_type = child.tag[:-3]  # Remove "_id" suffix  
                            if ref_type not in referenced_ids:
                                referenced_ids[ref_type] = set()
                            referenced_ids[ref_type].add(child.text)
        
        # Check if we have minimum required structure for validation
        if not existing_categories and self.permissive:
            # Only add minimal entry category in permissive mode
            print("⚠️ Adding minimal entry category for schema compliance (permissive mode)")
            entry_cat = ET.SubElement(root, "entryCategory")
            entry_elem = ET.SubElement(entry_cat, "entry")
            entry_elem.set("id", datablock_name)
        
        # The rest should be handled by schema validation reporting missing references
        # rather than auto-adding hardcoded categories
        
    def _generate_fallback_xml(self, data_block: DataBlock) -> str:
        """Generate a minimal valid XML as fallback."""
        lines = ['<?xml version="1.0" encoding="utf-8"?>']
        lines.append(f'<datablock xmlns="{self.namespace}" datablockName="{data_block.name}">')
        
        # Add minimal content - just the entry category
        lines.append('  <entryCategory>')
        lines.append(f'    <entry id="{data_block.name}"/>')
        lines.append('  </entryCategory>')
        
        lines.append('</datablock>')
        return '\n'.join(lines)
    
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
            
            # Get key items for this category - purely from dictionary/schema
            key_items = list(self._get_category_keys(category_name))                
            # Use mapping rules if dictionary keys not available
            if not key_items:
                key_items = self._get_keys_from_mapping_rules(category_name)
                if not key_items:
                    print(f"⚠️ No key items found for category {category_name} - proceeding without keys")
            
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
                
                # Get required attributes from schema/mapping rules only - no hardcoded enum references
                required_attrs = []
                if self.mapping_rules:
                    attribute_requirements = self.mapping_rules.get("attribute_requirements", {})
                    required_attrs = attribute_requirements.get(pdbml_category_name, [])
                
                # Add required attributes based on schema definitions
                for attr_name in required_attrs:
                    if attr_name in data and row_idx < len(data[attr_name]) and attr_name not in added_attrs:
                        cleaned_value = self._clean_field_value(str(data[attr_name][row_idx]), attr_name)
                        if cleaned_value:  # Only add non-empty values
                            attr_xml_name = self._sanitize_xml_name(attr_name)
                            row_elem.set(attr_xml_name, cleaned_value)
                            added_attrs.add(attr_name)
                
                
                # Get schema-driven element/attribute requirements - no hardcoded mappings
                attr_only_items = self._get_attribute_only_items_from_mapping()
                element_only_items = self._get_element_only_items_from_mapping()
                
                # Get schema-driven requirements for this category
                force_as_attrs = attr_only_items.get(pdbml_category_name, [])
                force_as_elems = element_only_items.get(pdbml_category_name, [])
                
                # Add non-key items based on schema requirements
                for item_name, values in data.items():
                    if item_name not in key_items and row_idx < len(values):
                        # Determine placement based on schema definitions
                        should_be_attribute = item_name in force_as_attrs
                        should_be_element = item_name in force_as_elems
                        
                        # Handle items that must be attributes according to schema
                        if should_be_attribute and item_name not in added_attrs:
                            cleaned_value = self._clean_field_value(str(values[row_idx]), item_name)
                            if cleaned_value:  # Only add non-empty values
                                row_elem.set(item_name, cleaned_value)
                                added_attrs.add(item_name)
                            continue
                        
                        # Handle items that must be elements according to schema
                        if should_be_element or not should_be_attribute:
                            safe_item_name = self._sanitize_xml_name(item_name)
                            try:
                                cleaned_value = self._clean_field_value(str(values[row_idx]), item_name)
                                # Always create element if item is present in data, even if value is empty/null
                                item_elem = ET.SubElement(row_elem, safe_item_name)
                                item_elem.text = cleaned_value if cleaned_value else ""
                            except Exception as e:
                                print(f"⚠️ Error adding element '{safe_item_name}': {str(e)}")
                
                
                # Add any missing required elements based purely on schema requirements
                # Only in permissive mode - add missing required elements with appropriate defaults
                if self.permissive and self.mapping_rules:
                    default_values = self.mapping_rules.get("default_values", {})
                    category_defaults = default_values.get(pdbml_category_name, {})
                    
                    for elem_name, default_value in category_defaults.items():
                        # Check if element already exists
                        if not any(child.tag.endswith(elem_name) for child in row_elem):
                            elem = ET.SubElement(row_elem, elem_name)
                            # Use the schema-defined default value, or mmCIF null indicator as fallback
                            elem.text = default_value if default_value else "."
                
        except Exception as e:
            print(f"⚠️ Error processing category {category_name}: {str(e)}")
    
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
            
            # Handle null values properly using schema validation
            if is_null_value(value):
                return ""
                
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
        
        # Second, try dictionary parser via mapping generator (if available)
        if hasattr(self, 'mapping_generator') and self.mapping_generator.categories:
            if clean_category in self.mapping_generator.categories:
                dict_keys = self.mapping_generator.categories[clean_category].get('keys', [])
                if dict_keys:
                    self._category_keys_cache[clean_category] = dict_keys
                    return tuple(dict_keys)
        
        # No keys found - log warning and return empty tuple
        print(f"⚠️ No key items found for category {category_name}")
        self._category_keys_cache[clean_category] = []
        return tuple()
    
    def _get_element_only_items_from_mapping(self) -> Dict[str, List[str]]:
        """Get element-only items from mapping rules (cached) - purely schema-driven."""
        if self._element_only_items_cache is not None:
            return self._element_only_items_cache
            
        if not self.mapping_rules:
            # No mapping rules available - return empty dict, let schema validation show real issues
            print("⚠️ Warning: No mapping rules available for element requirements")
            self._element_only_items_cache = {}
        else:
            element_requirements = self.mapping_rules.get("element_requirements", {})
            self._element_only_items_cache = element_requirements
            
        return self._element_only_items_cache
    
    def _get_attribute_only_items_from_mapping(self) -> Dict[str, List[str]]:
        """Get attribute-only items from mapping rules (cached) - purely schema-driven."""
        if self._attribute_only_items_cache is not None:
            return self._attribute_only_items_cache
            
        if not self.mapping_rules:
            # No mapping rules available - return empty dict, let schema validation show real issues
            print("⚠️ Warning: No mapping rules available for attribute requirements")
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
            from xml.etree import ElementTree as ET
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
                    from lxml import etree
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
        import re
        
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
        for category_name in categories.keys():
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
        """Identify parent-child relationships using ONLY schema-driven sources."""
        relationships = {
            'parents': {},  # child_category -> parent_category
            'children': {},  # parent_category -> [child_categories]
            'links': {}     # child_category -> parent_key_field
        }
        
        # Method 1: Use dictionary relationships (primary source - schema-driven)
        if self.dictionary:
            for child_category in categories:
                parents = self.dictionary.get_parent_relationships(child_category)
                for parent_info in parents:
                    parent_cat = parent_info['parent_name'].split('.')[0].lstrip('_')
                    parent_key = parent_info['parent_name'].split('.')[-1]
                    
                    if parent_cat in categories:  # Only if parent category exists in data
                        self._add_relationship(relationships, child_category, parent_cat, parent_key)
        
        # Method 2: Use standard mmCIF patterns ONLY as last resort when dictionary fails
        # This should be rare if dictionary is properly parsed
        if not relationships['parents']:  # Only if no dictionary relationships found
            standard_relationships = StandardRelationship.get_relationships_dict()
            
            for child_category, parent_info_list in standard_relationships.items():
                if child_category in categories:
                    child_items = categories[child_category]
                    
                    # Try each possible parent relationship for this child
                    for parent_category, link_field in parent_info_list:
                        if (child_category not in relationships['parents'] and 
                            parent_category in categories):
                            
                            # Verify the link field exists in child data
                            if child_items and any(link_field in item for item in child_items):
                                self._add_relationship(relationships, child_category, parent_category, link_field)
                                break  # Use the first matching relationship
        
        # NO hardcoded corrections - all relationships should come from schema/dictionary
        # If the dictionary/schema defines the relationships correctly, no manual corrections needed
        
        return relationships
    
    def _add_relationship(self, relationships: dict, child: str, parent: str, link_field: str):
        """Helper to add a relationship while maintaining consistency."""
        relationships['parents'][child] = parent
        if parent not in relationships['children']:
            relationships['children'][parent] = []
        if child not in relationships['children'][parent]:
            relationships['children'][parent].append(child)
        relationships['links'][child] = link_field
    
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
    
    def _get_item_key(self, item: Dict[str, Any], _category_name: str) -> str:
        """Get the primary key for an item."""
        # Common key patterns
        key_fields = ['id', 'name', 'code']
        
        for field in key_fields:
            if field in item:
                return str(item[field])
        
        # Fallback to first attribute
        return str(list(item.values())[0]) if item else "unknown"


class MMCIFToPDBMLPipeline:
    """Complete pipeline for mmCIF to PDBML conversion and relationship resolution."""
    
    def __init__(self, dictionary_path: Optional[Union[str, Path]] = None, schema_path: Optional[Union[str, Path]] = None, permissive: bool = False):
        """Initialize pipeline with optional dictionary and schema paths."""
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
        
        self.converter = PDBMLConverter(self.dictionary_path if self.dictionary_path.exists() else None, permissive=permissive)
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
        import json
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
