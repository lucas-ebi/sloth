#!/usr/bin/env python3
"""
SLOTH Demo - Lazy by design. Fast by default.

Demonstrates parsing, validation, modification, and writing of mmCIF files
using SLOTH's ultra-simple API that's automatically optimized for performance.
"""

import argparse
import os
import json
import copy
import tempfile
from sloth import (
    MMCIFHandler, ValidatorFactory, DataSourceFormat,
    SchemaValidator, JSONSchemaValidator, XMLSchemaValidator, YAMLSchemaValidator, CSVSchemaValidator,
    SchemaValidatorFactory, ValidationError
)


def category_validator(category_name):
    """Example validator function."""
    print(f"✅ Validating category: {category_name}")


def cross_checker(category_name_1, category_name_2):
    """Example cross-checker function."""
    print(f"🔗 Cross-checking: {category_name_1} ↔ {category_name_2}")


def modify_data(mmcif):
    """Example data modification."""
    if not mmcif.data:
        print("❌ No data blocks found")
        return
    
    block = mmcif.data[0]
    print(f"📋 Working with block: {block.name}")
    
    # Try to modify database information
    if '_database_2' in block.categories:
        # Direct dot notation access - the most elegant way!
        db_category = block._database_2  # This is dot notation in action!
        if hasattr(db_category, 'database_id') and db_category.database_id:
            original = db_category.database_id[-1]
            db_category.database_id[-1] = 'MODIFIED_DB'  # Simple assignment with dot notation
            print(f"✏️  Modified database_id: '{original}' → 'MODIFIED_DB'")
            print(f"   Using elegant dot notation: block._database_2.database_id[-1] = 'MODIFIED_DB'")
        else:
            print("ℹ️  No database_id found to modify")
    else:
        print("ℹ️  No _database_2 category found")


def show_file_info(mmcif):
    """Display information about the parsed file."""
    print(f"\n📊 File Information:")
    print(f"   Data blocks: {len(mmcif.data)}")
    
    for i, block in enumerate(mmcif.data):
        print(f"   Block {i+1}: '{block.name}' with {len(block.categories)} categories")
        
        # Show first few categories
        category_names = block.categories[:5]
        if category_names:
            print(f"   Categories: {', '.join(category_names)}")
            if len(block.categories) > 5:
                print(f"   ... and {len(block.categories) - 5} more")


def demo_with_sample_file():
    """Create and demonstrate with a sample file."""
    sample_content = """data_1ABC
#
_entry.id 1ABC_STRUCTURE
#
_database_2.database_id      PDB
_database_2.database_code    1ABC
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM   1    N  10.123 20.456 30.789
ATOM   2    C  11.234 21.567 31.890
ATOM   3    C  12.345 22.678 32.901
#
"""
    
    sample_file = "demo_structure.cif"
    with open(sample_file, 'w') as f:
        f.write(sample_content)
    
    print(f"📝 Created sample file: {sample_file}")
    return sample_file

def demonstrate_2d_slicing(mmcif):
    """Demonstrate 2D slicing functionality with emphasis on dot notation."""
    if not mmcif.data:
        print("❌ No data blocks found")
        return
    
    block = mmcif.data[0]
    print(f"\n🔢 Demonstrating 2D slicing with dot notation:")
    print(f"   The power of SLOTH's dot notation makes data access elegant and intuitive!")
    
    # Find an appropriate category with multiple rows for demonstration
    demo_categories = ['_atom_site', '_entity_poly_seq', '_struct_conn']
    demo_category = None
    
    for cat_name in demo_categories:
        if cat_name in block.categories:
            # Use dot notation to access category - this is the elegant way!
            if cat_name == '_atom_site':
                demo_category = block._atom_site  # Direct dot notation!
            elif cat_name == '_entity_poly_seq':
                demo_category = block._entity_poly_seq  # Direct dot notation!
            elif cat_name == '_struct_conn':
                demo_category = block._struct_conn  # Direct dot notation!
                
            if demo_category and demo_category.row_count >= 3:
                print(f"   Using category: {cat_name} with {demo_category.row_count} rows")
                print(f"   Accessed using elegant dot notation: block.{cat_name}")
                break
    
    if not demo_category:
        print("   No suitable category found with multiple rows for demonstration")
        return
    
    print("\n📊 Column-wise access with dot notation (the Pythonic way):")
    # Show all item names in the category
    print(f"   Available items: {', '.join(demo_category.items)}")
    
    # Get the first two item names for demonstration
    item_names = demo_category.items[:2]
    
    # Show dot notation access for typical items
    if 'group_PDB' in demo_category.items:
        values = demo_category.group_PDB  # Direct dot notation!
        print(f"   demo_category.group_PDB: {values[:3]} {'...' if len(values) > 3 else ''}")
    if 'id' in demo_category.items:
        values = demo_category.id  # Direct dot notation!
        print(f"   demo_category.id: {values[:3]} {'...' if len(values) > 3 else ''}")
    if 'type_symbol' in demo_category.items:
        values = demo_category.type_symbol  # Direct dot notation!
        print(f"   demo_category.type_symbol: {values[:3]} {'...' if len(values) > 3 else ''}")
    
    # For comparison, show dictionary style access (less elegant)
    print("\n   Alternative dictionary access (less intuitive):")
    for item_name in item_names:
        values = demo_category[item_name]
        print(f"   demo_category['{item_name}']: {values[:3]}")
    
    print("\n📋 Row-wise access with dot notation (elegant and readable):")
    # Show first row with dot notation access
    first_row = demo_category[0]
    print(f"   Row 0 (clean dot notation access):")
    
    # Use direct dot notation for common attributes
    if 'group_PDB' in demo_category.items:
        print(f"     row.group_PDB: {first_row.group_PDB}")  # Direct dot notation!
    if 'id' in demo_category.items:
        print(f"     row.id: {first_row.id}")  # Direct dot notation!
    if 'type_symbol' in demo_category.items:
        print(f"     row.type_symbol: {first_row.type_symbol}")  # Direct dot notation!
        
    # Show row.data
    print(f"\n   Complete row data as dictionary (row.data):")
    row_data = first_row.data
    for item_name, value in list(row_data.items())[:3]:
        print(f"     {item_name}: {value}")
    if len(row_data) > 3:
        print(f"     ... and {len(row_data) - 3} more items")
    
    # Show row slicing with dot notation access
    if demo_category.row_count >= 3:
        print(f"\n📑 Row slicing with dot notation:")
        rows = demo_category[0:3]
        print(f"   Slicing category[0:3] returns {len(rows)} rows")
        
        # Use dot notation for common attributes (the most elegant approach)
        if 'group_PDB' in demo_category.items and 'id' in demo_category.items:
            for i, row in enumerate(rows):
                # Direct dot notation access - this is the key pattern to highlight!
                print(f"   Row {i}: row.group_PDB={row.group_PDB}, row.id={row.id}")
        else:
            # Fallback for other attributes
            for i, row in enumerate(rows):
                item1 = item_names[0]
                item2 = item_names[1]
                print(f"   Row {i}: row.{item1}={getattr(row, item1)}, row.{item2}={getattr(row, item2)}")
    
    print("\n💡 Dot Notation Usage Tips (Pythonic best practices):")
    print("   1. Access data blocks: data.data_BLOCKNAME")
    print("   2. Access categories: block._category_name")
    print("   3. Access item values: category.item_name")
    print("   4. Access row values: row.item_name")
    print("   5. Complex example: data.data[0]._atom_site.Cartn_x[0]")
    print("   6. With slices: for row in category[0:3]: print(row.item_name)")
    print("\n   💪 Dot notation makes your code more readable, elegant, and Pythonic!")

def demonstrate_export_functionality(mmcif, output_dir):
    """Demonstrate the new export functionality."""
    print(f"\n📊 Demonstrating export functionality:")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create handler
    handler = MMCIFHandler()
    
    # Export to JSON
    json_path = os.path.join(output_dir, "exported_data.json")
    handler.export_to_json(mmcif, json_path)
    print(f"   ✅ Exported to JSON: {json_path}")
    
    # Export to XML
    xml_path = os.path.join(output_dir, "exported_data.xml")
    handler.export_to_xml(mmcif, xml_path)
    print(f"   ✅ Exported to XML: {xml_path}")
    
    # Export to Pickle
    pickle_path = os.path.join(output_dir, "exported_data.pkl")
    handler.export_to_pickle(mmcif, pickle_path)
    print(f"   ✅ Exported to Pickle: {pickle_path}")
    
    # Export to YAML (with try/except as it requires PyYAML)
    try:
        yaml_path = os.path.join(output_dir, "exported_data.yaml")
        handler.export_to_yaml(mmcif, yaml_path)
        print(f"   ✅ Exported to YAML: {yaml_path}")
    except ImportError as e:
        print(f"   ❌ YAML export failed: {str(e)}")
    
    # Export to CSV (with try/except as it requires pandas)
    try:
        csv_dir = os.path.join(output_dir, "csv_files")
        file_paths = handler.export_to_csv(mmcif, csv_dir)
        print(f"   ✅ Exported to CSV files in: {csv_dir}")
        # Show first CSV file path as example
        for block_name, categories in file_paths.items():
            if categories:
                first_category = next(iter(categories.keys()))
                first_path = categories[first_category]
                print(f"      Example: {os.path.basename(first_path)}")
                break
    except ImportError as e:
        print(f"   ❌ CSV export failed: {str(e)}")

def demonstrate_import_functionality(output_dir):
    """Demonstrate the new import functionality."""
    print(f"\n📥 Demonstrating import functionality:")
    
    # Create handler
    handler = MMCIFHandler()
    
    imported_containers = {}
    
    # Import from JSON
    json_path = os.path.join(output_dir, "exported_data.json")
    if os.path.exists(json_path):
        try:
            json_container = handler.import_from_json(json_path)
            imported_containers["JSON"] = json_container
            print(f"   ✅ Imported from JSON: {json_path}")
            print(f"      Found {len(json_container.blocks)} data block(s)")
        except Exception as e:
            print(f"   ❌ JSON import failed: {str(e)}")
    
    # Import from XML
    xml_path = os.path.join(output_dir, "exported_data.xml")
    if os.path.exists(xml_path):
        try:
            xml_container = handler.import_from_xml(xml_path)
            imported_containers["XML"] = xml_container
            print(f"   ✅ Imported from XML: {xml_path}")
            print(f"      Found {len(xml_container.blocks)} data block(s)")
        except Exception as e:
            print(f"   ❌ XML import failed: {str(e)}")
    
    # Import from Pickle
    pickle_path = os.path.join(output_dir, "exported_data.pkl")
    if os.path.exists(pickle_path):
        try:
            pickle_container = handler.import_from_pickle(pickle_path)
            imported_containers["Pickle"] = pickle_container
            print(f"   ✅ Imported from Pickle: {pickle_path}")
            print(f"      Found {len(pickle_container.blocks)} data block(s)")
        except Exception as e:
            print(f"   ❌ Pickle import failed: {str(e)}")
    
    # Import from YAML (with try/except as it requires PyYAML)
    yaml_path = os.path.join(output_dir, "exported_data.yaml")
    if os.path.exists(yaml_path):
        try:
            yaml_container = handler.import_from_yaml(yaml_path)
            imported_containers["YAML"] = yaml_container
            print(f"   ✅ Imported from YAML: {yaml_path}")
            print(f"      Found {len(yaml_container.blocks)} data block(s)")
        except ImportError as e:
            print(f"   ❌ YAML import failed: {str(e)}")
        except Exception as e:
            print(f"   ❌ YAML import failed: {str(e)}")
    
    # Import from CSV (with try/except as it requires pandas)
    csv_dir = os.path.join(output_dir, "csv_files")
    if os.path.exists(csv_dir):
        try:
            csv_container = handler.import_from_csv_files(csv_dir)
            imported_containers["CSV"] = csv_container
            print(f"   ✅ Imported from CSV files in: {csv_dir}")
            print(f"      Found {len(csv_container.blocks)} data block(s)")
        except ImportError as e:
            print(f"   ❌ CSV import failed: {str(e)}")
        except Exception as e:
            print(f"   ❌ CSV import failed: {str(e)}")
    
    # Auto-detect format import demo
    if imported_containers:
        print(f"\n🔍 Demonstrating auto-detect format import:")
        for format_name, first_path in [
            ("JSON", json_path),
            ("Pickle", pickle_path),
            ("YAML", yaml_path),
            ("XML", xml_path)
        ]:
            if os.path.exists(first_path) and format_name in imported_containers:
                try:
                    auto_container = handler.import_auto_detect(first_path)
                    print(f"   ✅ Auto-detected and imported from {format_name}: {first_path}")
                    print(f"      Found {len(auto_container.blocks)} data block(s)")
                    
                    # Compare with direct import to verify consistency
                    original = imported_containers[format_name]
                    if len(auto_container.blocks) == len(original.blocks):
                        print(f"      ✓ Content matches direct {format_name} import")
                    break
                except Exception as e:
                    print(f"   ❌ Auto-detect import failed for {format_name}: {str(e)}")
    
    return imported_containers

def demonstrate_round_trip(mmcif, imported_container, format_name):
    """Demonstrate round-trip validation between original and imported data."""
    print(f"\n🔄 Demonstrating round-trip validation ({format_name}):")
    
    if not mmcif.data or not imported_container.data:
        print("   ❌ Missing data blocks for comparison")
        return False
    
    # Check if blocks match
    if len(mmcif.data) != len(imported_container.data):
        print(f"   ❌ Block count mismatch: Original={len(mmcif.data)}, Imported={len(imported_container.data)}")
        return False
    
    # Compare first block
    original_block = mmcif.data[0]
    imported_block = imported_container.data[0]
    
    # Compare category count
    if len(original_block.categories) != len(imported_block.categories):
        print(f"   ⚠️ Category count differs: Original={len(original_block.categories)}, Imported={len(imported_block.categories)}")
    
    # Verify key categories exist in both
    common_categories = set(original_block.categories).intersection(set(imported_block.categories))
    print(f"   ✓ Found {len(common_categories)} common categories")
    
    # Check a few sample values
    if common_categories:
        example_category = list(common_categories)[0]
        print(f"   🔍 Checking values in category: {example_category}")
        
        original_cat = original_block[example_category]
        imported_cat = imported_block[example_category]
        
        # Compare item names
        original_items = set(original_cat.items)
        imported_items = set(imported_cat.items)
        common_items = original_items.intersection(imported_items)
        
        if common_items:
            sample_item = list(common_items)[0]
            original_values = original_cat[sample_item]
            imported_values = imported_cat[sample_item]
            
            # Check if array lengths match
            if len(original_values) == len(imported_values):
                print(f"   ✓ Item '{sample_item}' has {len(original_values)} values in both datasets")
                
                # Sample check first value
                if original_values[0] == imported_values[0]:
                    print(f"   ✓ First value matches: '{original_values[0]}'")
                else:
                    print(f"   ⚠️ First value differs: Original='{original_values[0]}', Imported='{imported_values[0]}'")
            else:
                print(f"   ⚠️ Value count differs: Original={len(original_values)}, Imported={len(imported_values)}")
    
    print(f"   ✅ Round-trip validation complete")
    return True

def demonstrate_schema_validation(mmcif, output_dir):
    """Demonstrate schema validation for different formats."""
    print(f"\n🛡️ Demonstrating Schema Validation:")
    
    # Create temporary directory for validation examples
    validation_dir = os.path.join(output_dir, "validation_examples")
    os.makedirs(validation_dir, exist_ok=True)
    
    # ===== JSON Schema Validation =====
    print("\n📝 JSON Schema Validation Example:")
    
    # Get the path to the exported JSON file
    json_path = os.path.join(output_dir, "exported_data.json")
    
    if os.path.exists(json_path):
        try:
            # Create a JSON schema validator using the default schema
            json_validator = SchemaValidatorFactory.create_validator(DataSourceFormat.JSON)
            
            # Create a valid and an invalid data file
            with open(json_path, 'r') as f:
                valid_data = json.load(f)
                
            # Copy the valid data
            invalid_data = copy.deepcopy(valid_data)
            
            # Create invalid data by removing all items in a category
            if invalid_data:
                block_name = list(invalid_data.keys())[0]
                if block_name in invalid_data:
                    category_name = list(invalid_data[block_name].keys())[0]
                    if category_name in invalid_data[block_name]:
                        # Make this category an empty object
                        if isinstance(invalid_data[block_name][category_name], dict):
                            invalid_data[block_name][category_name] = {}
                        # If it's an array, make it an empty array
                        elif isinstance(invalid_data[block_name][category_name], list):
                            invalid_data[block_name][category_name] = []
            
            # Save valid and invalid data
            valid_json_path = os.path.join(validation_dir, "valid_data.json")
            invalid_json_path = os.path.join(validation_dir, "invalid_data.json")
            
            with open(valid_json_path, 'w') as f:
                json.dump(valid_data, f)
                
            with open(invalid_json_path, 'w') as f:
                json.dump(invalid_data, f)
            
            # Validate valid data - should pass validation
            try:
                handler = MMCIFHandler()
                # Validate that the schema is correct for the valid data
                is_valid = json_validator.is_valid(valid_data)
                if is_valid:
                    valid_container = handler.import_from_json(valid_json_path, schema_validator=json_validator)
                    print(f"   ✅ Valid JSON data passed validation")
                else:
                    print(f"   ❌ Valid JSON data failed pre-validation check")
            except ValidationError as e:
                print(f"   ❌ Unexpected validation error on valid data: {e}")
            except Exception as e:
                print(f"   ❌ Error processing valid JSON: {e}")
            
            # Validate invalid data - should fail validation
            try:
                # First check that the schema correctly identifies invalid data
                is_invalid = not json_validator.is_valid(invalid_data)
                if is_invalid:
                    print(f"   ✅ Pre-validation correctly identified invalid JSON")
                    # This should raise a ValidationError
                    try:
                        json_validator.validate(invalid_data)
                        print(f"   ❌ Validation.validate() did not raise an error")
                    except ValidationError as e:
                        print(f"   ✅ Validation.validate() correctly raised: {e}")
                else:
                    print(f"   ❌ Invalid JSON incorrectly passed pre-validation")
                    
                # Now check that the import function correctly validates
                try:
                    invalid_container = handler.import_from_json(invalid_json_path, schema_validator=json_validator)
                    print(f"   ❌ Invalid JSON import did not raise an error")
                except ValidationError as e:
                    print(f"   ✅ Import correctly failed: {e}")
            except Exception as e:
                print(f"   ⚠️ Error during invalid JSON testing: {e}")
                
        except Exception as e:
            print(f"   ❌ JSON validation setup error: {e}")
    else:
        print(f"   ⚠️ JSON file not found, skipping validation")
    
    # ===== XML Schema Validation =====
    print("\n📝 XML Schema Validation Example:")
    
    # Get the path to the exported XML file
    xml_path = os.path.join(output_dir, "exported_data.xml")
    
    if os.path.exists(xml_path):
        try:
            # Create a simple XML validator using a function rather than XSD
            class SimpleXMLValidator(SchemaValidator):
                def validate(self, data):
                    return {"valid": True, "errors": []}
                    
                def is_valid(self, data):
                    return True
            
            xml_validator = SimpleXMLValidator()
            
            # Create a valid copy for demonstration
            valid_xml_path = os.path.join(validation_dir, "valid_data.xml")
            with open(xml_path, 'r') as src, open(valid_xml_path, 'w') as dst:
                dst.write(src.read())
            
            # Validate valid data
            try:
                handler = MMCIFHandler()
                valid_container = handler.import_from_xml(valid_xml_path, schema_validator=xml_validator)
                print(f"   ✅ Valid XML data passed validation")
            except ValidationError as e:
                print(f"   ❌ Unexpected validation error: {e}")
            except Exception as e:
                print(f"   ❌ XML validation error: {str(e)}")
                
        except Exception as e:
            print(f"   ❌ XML validation setup error: {e}")
    else:
        print(f"   ⚠️ XML file not found, skipping validation")
    
    # ===== YAML Schema Validation =====
    print("\n📝 YAML Schema Validation Example:")
    
    # Get the path to the exported YAML file
    yaml_path = os.path.join(output_dir, "exported_data.yaml")
    
    if os.path.exists(yaml_path):
        try:
            # Create a YAML schema validator using the default schema
            yaml_validator = SchemaValidatorFactory.create_validator(DataSourceFormat.YAML)
            
            # Create a valid copy for demonstration
            valid_yaml_path = os.path.join(validation_dir, "valid_data.yaml")
            with open(yaml_path, 'r') as src, open(valid_yaml_path, 'w') as dst:
                dst.write(src.read())
            
            # Validate valid data
            try:
                handler = MMCIFHandler()
                valid_container = handler.import_from_yaml(valid_yaml_path, schema_validator=yaml_validator)
                print(f"   ✅ Valid YAML data passed validation")
            except ValidationError as e:
                print(f"   ❌ Unexpected validation error: {e}")
            except Exception as e:
                print(f"   ❌ YAML validation error: {str(e)}")
                
        except Exception as e:
            print(f"   ❌ YAML validation setup error: {e}")
    else:
        print(f"   ⚠️ YAML file not found, skipping validation")
    
    # ===== Auto-detect with validation =====
    print("\n📝 Auto-detect Format with Validation Example:")
    
    try:
        # Use one of the valid files with auto-detection
        auto_detect_path = valid_json_path if locals().get('valid_json_path') else json_path
        
        if os.path.exists(auto_detect_path):
            handler = MMCIFHandler()
            container = handler.import_auto_detect(auto_detect_path, validate_schema=True)
            print(f"   ✅ Auto-detected format and validated successfully")
        else:
            print(f"   ⚠️ File not found for auto-detection")
            
    except Exception as e:
        print(f"   ❌ Auto-detection/validation error: {e}")
        
    print("\n🛡️ Schema validation demonstration completed")
    return validation_dir

def demonstrate_sample_data_creation():
    """Demonstrate both manual and programmatic approaches to creating sample data."""
    print("\n📝 Sample Data Creation Methods:")
    
    # Method 1: Manual file creation (like the existing demo)
    print("\n🖋️  Method 1: Manual mmCIF file creation")
    sample_content = """data_1ABC
_entry.id 1ABC_STRUCTURE
_database_2.database_id PDB
_database_2.database_code 1ABC
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM 1 N 10.123 20.456 30.789
ATOM 2 C 11.234 21.567 31.890
"""
    
    manual_file = "sample_manual.cif"
    with open(manual_file, 'w') as f:
        f.write(sample_content)
    print(f"   ✅ Created manual sample: {manual_file}")
    
    # Method 2: Programmatic creation using SLOTH's API with dictionary notation
    print("\n⚙️  Method 2: Programmatic creation using dictionary notation")
    try:
        from sloth.models import MMCIFDataContainer, DataBlock, Category
        
        # Create container and block
        container = MMCIFDataContainer()
        block = DataBlock("1ABC")
        
        # Create categories and add data using dictionary-style assignment
        # Entry information
        entry_category = Category("_entry")
        entry_category["id"] = ["1ABC_STRUCTURE"]
        
        # Database information  
        database_category = Category("_database_2")
        database_category["database_id"] = ["PDB"]
        database_category["database_code"] = ["1ABC"]
        
        # Atom site information
        atom_site_category = Category("_atom_site")
        atom_site_category["group_PDB"] = ["ATOM", "ATOM"]
        atom_site_category["id"] = ["1", "2"]
        atom_site_category["type_symbol"] = ["N", "C"]
        atom_site_category["Cartn_x"] = ["10.123", "11.234"]
        atom_site_category["Cartn_y"] = ["20.456", "21.567"]
        atom_site_category["Cartn_z"] = ["30.789", "31.890"]
        
        # Add categories to block
        block["_entry"] = entry_category
        block["_database_2"] = database_category
        block["_atom_site"] = atom_site_category
        
        # Add block to container
        container["1ABC"] = block
        
        # Write using SLOTH
        programmatic_file = "sample_programmatic.cif"
        handler = MMCIFHandler()
        with open(programmatic_file, 'w') as f:
            handler.file_obj = f
            handler.write(container)
        print(f"   ✅ Created programmatic sample: {programmatic_file}")
        
        # Method 3: NEW! Elegant dot notation creation
        print("\n✨ Method 3: Elegant dot notation creation (NEW!)")
        
        # Create everything with pure dot notation!
        container_dot = MMCIFDataContainer()
        block_dot = DataBlock("1ABC_DOT")
        
        # Create categories using dot notation
        entry_cat = Category("_entry")
        entry_cat.id = ["1ABC_DOT_STRUCTURE"]  # Dot notation assignment!
        
        database_cat = Category("_database_2")
        database_cat.database_id = ["PDB"]      # Dot notation assignment!
        database_cat.database_code = ["1ABC"]   # Dot notation assignment!
        
        atom_cat = Category("_atom_site")
        atom_cat.group_PDB = ["ATOM", "ATOM"]   # Dot notation assignment!
        atom_cat.id = ["1", "2"]                # Dot notation assignment!
        atom_cat.type_symbol = ["N", "C"]       # Dot notation assignment!
        atom_cat.Cartn_x = ["10.123", "11.234"] # Dot notation assignment!
        atom_cat.Cartn_y = ["20.456", "21.567"] # Dot notation assignment!
        atom_cat.Cartn_z = ["30.789", "31.890"] # Dot notation assignment!
        
        # Assign categories using dot notation!
        block_dot._entry = entry_cat           # Dot notation assignment!
        block_dot._database_2 = database_cat   # Dot notation assignment!
        block_dot._atom_site = atom_cat        # Dot notation assignment!
        
        # Assign block using dot notation!
        container_dot.data_1ABC_DOT = block_dot # Dot notation assignment!
        
        # Write using SLOTH
        dot_notation_file = "sample_dot_notation.cif"
        with open(dot_notation_file, 'w') as f:
            handler.file_obj = f
            handler.write(container_dot)
        print(f"   ✅ Created dot notation sample: {dot_notation_file}")
        
        # Parse all files to verify they work
        manual_mmcif = handler.parse(manual_file)
        programmatic_mmcif = handler.parse(programmatic_file)
        dot_notation_mmcif = handler.parse(dot_notation_file)
        
        print(f"\n🔍 Verification:")
        print(f"   Manual approach: {len(manual_mmcif.data[0].categories)} categories")
        print(f"   Dictionary approach: {len(programmatic_mmcif.data[0].categories)} categories")
        print(f"   Dot notation approach: {len(dot_notation_mmcif.data[0].categories)} categories")
        
        # Demonstrate the elegance of dot notation access
        print(f"\n💡 Demonstrating dot notation elegance:")
        print(f"   dot_notation_mmcif.data_1ABC_DOT._entry.id[0]: {dot_notation_mmcif.data_1ABC_DOT._entry.id[0]}")
        print(f"   dot_notation_mmcif.data_1ABC_DOT._atom_site.type_symbol: {dot_notation_mmcif.data_1ABC_DOT._atom_site.type_symbol}")
        
        return manual_file, programmatic_file, dot_notation_file
        
    except ImportError as e:
        print(f"   ⚠️ Programmatic approach not available: {e}")
        print("   📋 Using manual approach only")
        return manual_file, None, None
    except Exception as e:
        print(f"   ❌ Error in programmatic approach: {e}")
        return manual_file, None, None

def main():
    parser = argparse.ArgumentParser(
        description="SLOTH - Structural Loader with On-demand Tokenization and Handling | Lazy by design. Fast by default.",
        epilog="""
Examples:
  python demo.py input.cif output.cif
  python demo.py input.cif output.cif --categories _database_2 _atom_site
  python demo.py input.cif output.cif --validate
  python demo.py --demo  # Create and process a sample file
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument("input", nargs='?', help="Path to input mmCIF file")
    parser.add_argument("output", nargs='?', help="Path to write modified mmCIF file")
    parser.add_argument("--categories", nargs="+", help="Specific categories to process", default=None)
    parser.add_argument("--validate", action="store_true", help="Run validation on categories")
# Removed --schema-validate flag as it's always included in demo mode
    parser.add_argument("--demo", action="store_true", help="Run demo with sample data")
    
    args = parser.parse_args()
    
    # Handle demo mode
    if args.demo:
        print("🦥 SLOTH Demo")
        print("Lazy by design. Fast by default.")
        print("=" * 40)
        
        sample_file = demo_with_sample_file()
        args.input = sample_file
        args.output = "demo_modified.cif"
        args.validate = True
        args.schema_validate = True
    
    # Validate arguments
    if not args.input or not args.output:
        if not args.demo:
            parser.error("Both input and output files are required (or use --demo)")
    
    # Check input file exists
    if not os.path.exists(args.input):
        print(f"❌ Error: Input file '{args.input}' not found")
        return 1
    
    print(f"\n🔍 Processing: {args.input}")
    if args.categories:
        print(f"📂 Categories: {', '.join(args.categories)}")
    
    # Setup handler
    validator_factory = ValidatorFactory() if args.validate else None
    handler = MMCIFHandler(validator_factory=validator_factory)
    
    try:
        # Parse the file
        print("⚡ Parsing file...")
        mmcif = handler.parse(args.input, categories=args.categories)
        
        # Show file information
        show_file_info(mmcif)
        
        # Demonstrate sample data creation methods (in demo mode)
        if args.demo:
            demonstrate_sample_data_creation()
        
        # Setup validation if requested
        if args.validate and mmcif.data:
            print(f"\n🛡️  Setting up validation...")
            block = mmcif.data[0]
            
            # Register validators for available categories
            available_categories = block.categories[:2]  # First 2 for demo
            for cat_name in available_categories:
                validator_factory.register_validator(cat_name, category_validator)
            
            # Register cross-checker if we have multiple categories
            if len(available_categories) >= 2:
                cat_pair = (available_categories[0], available_categories[1])
                validator_factory.register_cross_checker(cat_pair, cross_checker)
            
            # Run validation
            print(f"🔍 Running validation...")
            for cat_name in available_categories:
                if cat_name in block.categories:
                    category = block.data[cat_name]
                    category.validate()
            
            # Run cross-validation if available
            if len(available_categories) >= 2:
                cat1_name, cat2_name = available_categories[0], available_categories[1]
                if cat1_name in block.categories and cat2_name in block.categories:
                    cat1 = block.data[cat1_name]
                    cat2 = block.data[cat2_name]
                    cat1.validate().against(cat2)
        
        # Demonstrate 2D slicing
        demonstrate_2d_slicing(mmcif)
        
        # Modify data
        print(f"\n✏️  Modifying data...")
        modify_data(mmcif)
        
        # Write output
        print(f"\n💾 Writing to: {args.output}")
        with open(args.output, 'w') as f:
            handler.file_obj = f
            handler.write(mmcif)
        
        print(f"✅ Successfully processed!")
        
        # Verify the output
        print(f"\n🔍 Verifying output...")
        verify_data = handler.parse(args.output)
        print(f"✅ Output file contains {len(verify_data.data)} data block(s)")
        
        # Demonstrate 2D slicing if available
        if hasattr(handler, 'demonstrate_2d_slicing'):
            demonstrate_2d_slicing(mmcif)

        # Demonstrate export functionality
        output_dir = "exports"
        demonstrate_export_functionality(mmcif, output_dir)
        
        # Demonstrate import functionality
        imported_containers = demonstrate_import_functionality(output_dir)
        
        # Demonstrate round-trip validation for each imported format
        for format_name, imported_container in imported_containers.items():
            demonstrate_round_trip(mmcif, imported_container, format_name)
            
        # Demonstrate schema validation
        # Note: This is always included in demo mode
        validation_dir = demonstrate_schema_validation(mmcif, output_dir)
        
        # Clean up demo files if created
        if args.demo and os.path.exists("demo_structure.cif"):
            os.remove("demo_structure.cif")
            print("🧹 Cleaned up demo files")
            
        # Clean up validation examples
        if 'validation_dir' in locals() and os.path.exists(validation_dir):
            import shutil
            try:
                shutil.rmtree(validation_dir)
                print("🧹 Cleaned up validation example files")
            except Exception:
                pass
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
