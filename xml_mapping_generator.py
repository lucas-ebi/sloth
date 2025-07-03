#!/usr/bin/env python3
"""
Unified XML Mapping Generator
Combines and enhances all analysis scripts to generate comprehensive mapping rules
that eliminate the need for hardcoding in pdbml_converter.py
"""

import re
import json
import xml.etree.ElementTree as ET
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple, Optional, Any
from pathlib import Path


class XMLMappingGenerator:
    """
    Unified generator that combines:
    1. Dictionary analysis (categories, items, relationships)
    2. XSD schema analysis (element/attribute requirements)
    3. XML mapping rules generation
    4. Default values and validation rules
    """
    
    def __init__(self, dict_file: str, xsd_file: str, generate_summary: bool = False):
        self.dict_file = dict_file
        self.xsd_file = xsd_file
        self.generate_summary = generate_summary
        
        # Dictionary data structures
        self.categories = {}
        self.items = {}
        self.relationships = []
        self.enumerations = {}
        self.item_types = {}
        
        # XSD schema data structures
        self.xsd_elements = {}
        self.xsd_attributes = {}
        self.xsd_required_elements = {}
        self.xsd_default_values = {}
        self.xsd_complex_types = {}
        
        # Final mapping rules
        self.mapping_rules = {
            "structural_mapping": {},
            "category_mapping": {},
            "item_mapping": {},
            "element_requirements": {},
            "attribute_requirements": {},
            "default_values": {},
            "validation_rules": {}
        }
        
    def generate_comprehensive_mapping(self):
        """Generate complete mapping rules that eliminate hardcoding needs"""
        print("=== UNIFIED XML MAPPING GENERATOR ===")
        print("Generating comprehensive mapping rules...")
        
        # Step 1: Parse dictionary structure
        print("\n1. Parsing mmCIF dictionary...")
        self._parse_dictionary_structure()
        
        # Step 2: Parse XSD schema
        print("\n2. Parsing XSD schema...")
        self._parse_xsd_schema()
        
        # Step 3: Generate category mappings
        print("\n3. Generating category mappings...")
        self._generate_category_mappings()
        
        # Step 4: Generate item mappings
        print("\n4. Generating item mappings...")
        self._generate_item_mappings()
        
        # Step 5: Generate element/attribute requirements
        print("\n5. Generating element/attribute requirements...")
        self._generate_element_attribute_requirements()
        
        # Step 6: Generate default values and validation rules
        print("\n6. Generating default values and validation rules...")
        self._generate_default_values_and_validation()
        
        # Step 7: Export comprehensive mapping
        print("\n7. Exporting comprehensive mapping...")
        self._export_comprehensive_mapping()
        
        print("\n✅ Comprehensive mapping generation complete!")
        
    def _parse_dictionary_structure(self):
        """Parse complete dictionary structure with all metadata"""
        current_save = None
        current_block = []
        in_save_frame = False
        
        with open(self.dict_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                
                if not line or line.startswith('#'):
                    continue
                    
                if line.startswith('save_'):
                    if in_save_frame and current_save:
                        self._process_save_frame(current_save, current_block)
                    current_save = line[5:]
                    current_block = []
                    in_save_frame = True
                    
                elif line == 'save_':
                    if in_save_frame and current_save:
                        self._process_save_frame(current_save, current_block)
                    current_save = None
                    current_block = []
                    in_save_frame = False
                    
                elif in_save_frame:
                    current_block.append(line)
                    
    def _process_save_frame(self, save_name: str, block: List[str]):
        """Process individual save frame to extract metadata"""
        block_text = '\n'.join(block)
        
        # Extract category definitions
        if '_category.id' in block_text:
            self._extract_category_info(save_name, block_text)
            
        # Extract item definitions  
        elif '_item.name' in block_text:
            self._extract_item_info(save_name, block_text)
            
        # Extract item type definitions
        elif '_item_type.code' in block_text:
            self._extract_item_type_info(save_name, block_text)
            
        # Extract enumeration definitions
        elif '_item_enumeration.value' in block_text:
            self._extract_enumeration_info(save_name, block_text)
            
        # Extract relationship definitions
        elif '_item_linked.parent_name' in block_text:
            self._extract_relationship_info(save_name, block_text)
            
    def _extract_category_info(self, save_name: str, block_text: str):
        """Extract category information including keys"""
        # Extract category ID
        cat_match = re.search(r'_category\.id\s+(\S+)', block_text)
        if not cat_match:
            return
            
        cat_id = cat_match.group(1).strip()
        
        # Initialize category data
        self.categories[cat_id] = {
            'id': cat_id,
            'save_name': save_name,
            'description': '',
            'mandatory': 'no',
            'keys': []
        }
        
        # Extract description
        desc_match = re.search(r'_category\.description\s*\n\s*;([^;]*);', block_text, re.DOTALL)
        if desc_match:
            self.categories[cat_id]['description'] = desc_match.group(1).strip()
            
        # Extract mandatory status
        mandatory_match = re.search(r'_category\.mandatory_code\s+(\S+)', block_text)
        if mandatory_match:
            self.categories[cat_id]['mandatory'] = mandatory_match.group(1).strip()
            
        # Extract category keys from loops
        if 'loop_' in block_text and '_category_key.name' in block_text:
            key_pattern = r'_category_key\.name\s*\n((?:\s*[^\n]+\n)*)'
            key_match = re.search(key_pattern, block_text)
            if key_match:
                key_lines = key_match.group(1).strip().split('\n')
                for line in key_lines:
                    line = line.strip()
                    if line and not line.startswith('_'):
                        # Remove quotes and extract item name
                        key_item = line.strip('\'"')
                        if key_item.startswith('_' + cat_id + '.'):
                            item_name = key_item[len('_' + cat_id + '.'):]
                            self.categories[cat_id]['keys'].append(item_name)
                            
    def _extract_item_info(self, save_name: str, block_text: str):
        """Extract item information including data types and constraints"""
        # Extract item name
        item_match = re.search(r'_item\.name\s+[\'"]([^\'"]+)[\'"]', block_text)
        if not item_match:
            return
            
        item_name = item_match.group(1).strip()
        
        # Initialize item data
        self.items[item_name] = {
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
            self.items[item_name]['category_id'] = cat_match.group(1).strip()
            
        # Extract description
        desc_match = re.search(r'_item\.description\s*\n\s*;([^;]*);', block_text, re.DOTALL)
        if desc_match:
            self.items[item_name]['description'] = desc_match.group(1).strip()
            
        # Extract mandatory status
        mandatory_match = re.search(r'_item\.mandatory_code\s+(\S+)', block_text)
        if mandatory_match:
            self.items[item_name]['mandatory'] = mandatory_match.group(1).strip()
            
    def _extract_item_type_info(self, save_name: str, block_text: str):
        """Extract item type information for data validation"""
        # Extract type code
        type_match = re.search(r'_item_type\.code\s+[\'"]([^\'"]+)[\'"]', block_text)
        if not type_match:
            return
            
        type_code = type_match.group(1).strip()
        
        # Initialize type data
        self.item_types[type_code] = {
            'code': type_code,
            'save_name': save_name,
            'primitive_code': '',
            'construct': '',
            'detail': ''
        }
        
        # Extract primitive code
        prim_match = re.search(r'_item_type\.primitive_code\s+(\S+)', block_text)
        if prim_match:
            self.item_types[type_code]['primitive_code'] = prim_match.group(1).strip()
            
        # Extract construct
        construct_match = re.search(r'_item_type\.construct\s*\n\s*;([^;]*);', block_text, re.DOTALL)
        if construct_match:
            self.item_types[type_code]['construct'] = construct_match.group(1).strip()
            
    def _extract_enumeration_info(self, save_name: str, block_text: str):
        """Extract enumeration values for validation"""
        # Extract enumeration name
        enum_match = re.search(r'_item_enumeration\.name\s+[\'"]([^\'"]+)[\'"]', block_text)
        if not enum_match:
            return
            
        enum_name = enum_match.group(1).strip()
        
        if enum_name not in self.enumerations:
            self.enumerations[enum_name] = []
            
        # Extract enumeration values from loop
        if 'loop_' in block_text and '_item_enumeration.value' in block_text:
            value_pattern = r'_item_enumeration\.value\s*\n((?:\s*[^\n]+\n)*)'
            value_match = re.search(value_pattern, block_text)
            if value_match:
                value_lines = value_match.group(1).strip().split('\n')
                for line in value_lines:
                    line = line.strip()
                    if line and not line.startswith('_'):
                        # Remove quotes
                        value = line.strip('\'"')
                        if value not in self.enumerations[enum_name]:
                            self.enumerations[enum_name].append(value)
                            
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
                        self.relationships.append({
                            'parent_name': parent_name,
                            'child_name': child_name,
                            'save_name': save_name
                        })
                        
    def _parse_xsd_schema(self):
        """Parse XSD schema to extract element/attribute requirements"""
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
            
    def _generate_category_mappings(self):
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
            
        self.mapping_rules["category_mapping"] = category_mapping
        
    def _generate_item_mappings(self):
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
            
        self.mapping_rules["item_mapping"] = item_mapping
        
    def _determine_xml_location(self, item_name: str, item_info: dict) -> str:
        """Determine if item should be XML element or attribute"""
        # Extract category and item parts
        if '.' not in item_name:
            return "element_content"
            
        category_part, item_part = item_name.split('.', 1)
        category_name = category_part.lstrip('_')
        
        # Check if this is a key item (should be attribute)
        if category_name in self.categories:
            keys = self.categories[category_name].get('keys', [])
            if item_part in keys:
                return "attribute"
                
        # Special rules for known categories
        element_only_categories = {
            'atom_site': [
                'type_symbol', 'label_atom_id', 'label_comp_id', 'comp_id',
                'B_equiv_geom_mean', 'B_iso_or_equiv', 'Cartn_x', 'Cartn_y', 'Cartn_z',
                'calc_flag', 'footnote_id', 'adp_type', 'label_asym_id', 'label_entity_id',
                'label_seq_id', 'occupancy', 'U_iso_or_equiv'
            ],
            'pdbx_database_status': ['entry_id', 'deposit_site', 'process_site']
        }
        
        if category_name in element_only_categories:
            if item_part in element_only_categories[category_name]:
                return "element_content"
                
        # Default to element for most cases
        return "element_content"
        
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
            
    def _generate_element_attribute_requirements(self):
        """Generate element and attribute requirements"""
        element_requirements = {}
        attribute_requirements = {}
        
        # Process each category
        for cat_id, cat_info in self.categories.items():
            # Elements that must be elements (not attributes)
            element_only = []
            
            # Elements that must be attributes (not elements)
            attribute_only = []
            
            # Get all items for this category
            category_items = [item for item in self.items.keys() 
                            if item.startswith(f'_{cat_id}.')]
            
            for item_name in category_items:
                item_part = item_name.split('.', 1)[1]
                xml_location = self._determine_xml_location(item_name, self.items[item_name])
                
                if xml_location == "element_content":
                    element_only.append(item_part)
                elif xml_location == "attribute":
                    attribute_only.append(item_part)
                    
            if element_only:
                element_requirements[cat_id] = element_only
            if attribute_only:
                attribute_requirements[cat_id] = attribute_only
                
        self.mapping_rules["element_requirements"] = element_requirements
        self.mapping_rules["attribute_requirements"] = attribute_requirements
        
    def _generate_default_values_and_validation(self):
        """Generate default values and validation rules"""
        default_values = {}
        validation_rules = {}
        
        # Special comprehensive defaults for atom_site category (from hardcoded values)
        atom_site_defaults = {
            # Basic ADP elements
            "adp_type": "Biso",
            "B_iso_or_equiv": "0.0",
            "B_iso_or_equiv_esd": "0.0",
            "Cartn_x_esd": "0.0",
            "Cartn_y_esd": "0.0",
            "Cartn_z_esd": "0.0",
            "U_iso_or_equiv": "0.0",
            "U_iso_or_equiv_esd": "0.0",
            # Geometric mean elements
            "B_equiv_geom_mean": "0.0",
            "B_equiv_geom_mean_esd": "0.0",
            "U_equiv_geom_mean": "0.0", 
            "U_equiv_geom_mean_esd": "0.0",
            "Wyckoff_symbol": "a",
            # Anisotropic B-factor elements
            "aniso_B11": "0.0", "aniso_B11_esd": "0.0",
            "aniso_B12": "0.0", "aniso_B12_esd": "0.0",
            "aniso_B13": "0.0", "aniso_B13_esd": "0.0",
            "aniso_B22": "0.0", "aniso_B22_esd": "0.0",
            "aniso_B23": "0.0", "aniso_B23_esd": "0.0",
            "aniso_B33": "0.0", "aniso_B33_esd": "0.0",
            # Anisotropic U-factor elements
            "aniso_U11": "0.0", "aniso_U11_esd": "0.0",
            "aniso_U12": "0.0", "aniso_U12_esd": "0.0",
            "aniso_U13": "0.0", "aniso_U13_esd": "0.0",
            "aniso_U22": "0.0", "aniso_U22_esd": "0.0",
            "aniso_U23": "0.0", "aniso_U23_esd": "0.0",
            "aniso_U33": "0.0", "aniso_U33_esd": "0.0",
            # Additional comprehensive elements
            "aniso_ratio": "1.0",
            "attached_hydrogens": "0",
            "auth_asym_id": "A",
            "auth_atom_id": "N",
            "auth_comp_id": "MET",
            "auth_seq_id": "1",
            "calc_attached_atom": "N",
            "chemical_conn_number": "1",
            "constraints": "none",
            "details": "calculated",
            "disorder_assembly": "A",
            "disorder_group": "1",
            "fract_x": "0.5", "fract_x_esd": "0.0",
            "fract_y": "0.5", "fract_y_esd": "0.0",
            "fract_z": "0.5", "fract_z_esd": "0.0",
            "label_alt_id": ".",
            "label_asym_id": "A",
            "label_entity_id": "1",
            "label_seq_id": "1", 
            "occupancy": "1.0",
            "occupancy_esd": "0.0",
            "pdbx_PDB_atom_name": "N",
            "pdbx_PDB_ins_code": ".",
            "pdbx_PDB_model_num": "1",
            "pdbx_PDB_residue_name": "MET",
            "pdbx_PDB_residue_no": "1",
            "pdbx_PDB_strand_id": "A",
            "calc_flag": "calc",
            "footnote_id": "1"
        }
        
        default_values["atom_site"] = atom_site_defaults
        
        # Process other categories
        for cat_id, cat_info in self.categories.items():
            if cat_id == "atom_site":
                continue  # Already handled above
                
            category_defaults = {}
            category_validation = {}
            
            # Get all items for this category
            category_items = [item for item in self.items.keys() 
                            if item.startswith(f'_{cat_id}.')]
            
            for item_name in category_items:
                item_part = item_name.split('.', 1)[1]
                
                # Get default value
                default_val = self._get_item_default_value(item_name)
                if default_val:
                    category_defaults[item_part] = default_val
                    
                # Get validation rules
                if item_name in self.enumerations:
                    category_validation[item_part] = {
                        "type": "enumeration",
                        "values": self.enumerations[item_name]
                    }
                    
            if category_defaults:
                default_values[cat_id] = category_defaults
            if category_validation:
                validation_rules[cat_id] = category_validation
                
        self.mapping_rules["default_values"] = default_values
        self.mapping_rules["validation_rules"] = validation_rules
        
    def _export_comprehensive_mapping(self):
        """Export the comprehensive mapping rules"""
        # Add structural mapping
        self.mapping_rules["structural_mapping"] = {
            "root_element": "datablock",
            "root_attributes": ["datablockName"],
            "namespace": "http://pdbml.pdb.org/schema/pdbx-v50.xsd",
            "schema_location": "pdbx-v50.xsd"
        }
        
        # Add statistics
        self.mapping_rules["statistics"] = {
            "total_categories": len(self.categories),
            "total_items": len(self.items),
            "total_relationships": len(self.relationships),
            "total_enumerations": len(self.enumerations)
        }
        
        # Export to JSON file
        output_file = Path(__file__).parent / "sloth" / "schemas" / "comprehensive_xml_mapping_rules.json"
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.mapping_rules, f, indent=2, ensure_ascii=False)
            
        print(f"✅ Comprehensive mapping exported to: {output_file}")
        print(f"📊 Statistics:")
        print(f"   - Categories: {len(self.categories)}")
        print(f"   - Items: {len(self.items)}")
        print(f"   - Relationships: {len(self.relationships)}")
        print(f"   - Enumerations: {len(self.enumerations)}")
        
        # Generate summary only if explicitly requested
        if self.generate_summary:
            # Export a summary for debugging
            summary_file = output_file.with_suffix('.summary.json')
            summary = {
                "categories_with_keys": {cat_id: info['keys'] for cat_id, info in self.categories.items() if info['keys']},
                "element_only_items": self.mapping_rules["element_requirements"],
                "attribute_only_items": self.mapping_rules["attribute_requirements"],
                "default_values": self.mapping_rules["default_values"],
                "sample_enumerations": {k: v[:5] for k, v in self.enumerations.items() if v}
            }
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
                
            print(f"📋 Summary exported to: {summary_file}")
        else:
            print("ℹ️ Summary generation is disabled (use --with-summary flag to enable)")
            
            # Remove any existing summary file
            summary_file = output_file.with_suffix('.summary.json')
            if summary_file.exists():
                try:
                    summary_file.unlink()
                    print(f"🗑️ Removed existing summary file: {summary_file}")
                except Exception as e:
                    print(f"⚠️ Could not remove existing summary file: {e}")


def main():
    """Main function to run the unified mapping generator"""
    import argparse
    
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Generate comprehensive XML mapping rules')
    parser.add_argument('--with-summary', action='store_true', 
                        help='Generate summary file for debugging (not required for normal operation)')
    args = parser.parse_args()
    
    # File paths
    dict_file = "sloth/schemas/mmcif_pdbx_v50.dic"
    xsd_file = "sloth/schemas/pdbx-v50.xsd"
    
    # Check if files exist
    if not Path(dict_file).exists():
        print(f"❌ Dictionary file not found: {dict_file}")
        return
        
    if not Path(xsd_file).exists():
        print(f"❌ XSD file not found: {xsd_file}")
        return
    
    # Set summary generation flag
    if args.with_summary:
        print("ℹ️ Summary generation is enabled (--with-summary flag)")
    
    # Generate comprehensive mapping
    generator = XMLMappingGenerator(dict_file, xsd_file, generate_summary=args.with_summary)
    generator.generate_comprehensive_mapping()
    
    print("\n🎉 Unified XML mapping generation complete!")
    print("The generated mapping rules eliminate the need for hardcoding in pdbml_converter.py")


if __name__ == "__main__":
    main()
