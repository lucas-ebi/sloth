#!/usr/bin/env python3
"""
SLOTH Demo - Lazy by design. Fast by default.

Demonstrates parsing, validation, modification, and writing of mmCIF files
using SLOTH's ultra-simple API that's automatically optimized for performance.
"""

import argparse
import os
from . import MMCIFHandler, ValidatorFactory


def category_validator(category_name):
    """Example validator function."""
    print(f"✅ Validating category: {category_name}")


def cross_checker(category_name_1, category_name_2):
    """Example cross-checker function."""
    print(f"🔗 Cross-checking: {category_name_1} ↔ {category_name_2}")


def modify_data(data_container):
    """Example data modification."""
    if not data_container.data:
        print("❌ No data blocks found")
        return
    
    block = data_container.data[0]
    print(f"📋 Working with block: {block.name}")
    
    # Try to modify database information
    if '_database_2' in block.categories:
        category = block.data['_database_2']
        if hasattr(category, 'database_id') and category.database_id:
            original = category.database_id[-1]
            category.database_id[-1] = 'MODIFIED_DB'
            print(f"✏️  Modified database_id: '{original}' → 'MODIFIED_DB'")
        else:
            print("ℹ️  No database_id found to modify")
    else:
        print("ℹ️  No _database_2 category found")


def show_file_info(data_container):
    """Display information about the parsed file."""
    print(f"\n📊 File Information:")
    print(f"   Data blocks: {len(data_container.data)}")
    
    for i, block in enumerate(data_container.data):
        print(f"   Block {i+1}: '{block.name}' with {len(block.categories)} categories")
        
        # Show first few categories
        category_names = block.categories[:5]
        if category_names:
            print(f"   Categories: {', '.join(category_names)}")
            if len(block.categories) > 5:
                print(f"   ... and {len(block.categories) - 5} more")


def demo_with_sample_file():
    """Create and demonstrate with a sample file."""
    sample_content = """data_EXAMPLE
#
_entry.id EXAMPLE_STRUCTURE
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

def demonstrate_2d_slicing(data_container):
    """Demonstrate the new 2D slicing functionality and Row access patterns."""
    if not data_container.data:
        print("❌ No data blocks found")
        return
    
    block = data_container.data[0]
    print(f"\n🔢 Demonstrating 2D slicing functionality:")
    
    # Find an appropriate category with multiple rows for demonstration
    demo_categories = ['_atom_site', '_entity_poly_seq', '_struct_conn']
    demo_category = None
    
    for cat_name in demo_categories:
        if cat_name in block.categories and cat_name in block.data:
            category = block.data[cat_name]
            if category.row_count >= 3:  # Need at least 3 rows for good demo
                demo_category = category
                print(f"   Using category: {cat_name} with {category.row_count} rows")
                break
    
    if not demo_category:
        print("   No suitable category found with multiple rows for demonstration")
        return
    
    print("\n📊 Column-wise access (traditional way):")
    # Show all item names in the category
    print(f"   Available items: {', '.join(demo_category.items)}")
    
    # Get the first two item names for demonstration
    item_names = demo_category.items[:2]
    for item_name in item_names:
        values = demo_category[item_name]
        print(f"   {item_name}: {values[:3]} {'...' if len(values) > 3 else ''}")
    
    print("\n📋 Row-wise access (new 2D slicing):")
    # Show first row with attribute access
    first_row = demo_category[0]
    print(f"   Row 0 (attribute access):")
    for item_name in item_names:
        print(f"     {item_name}: {getattr(first_row, item_name)}")
    
    # Show second row with dictionary access
    second_row = demo_category[1]
    print(f"   Row 1 (dictionary access):")
    for item_name in item_names:
        print(f"     {item_name}: {second_row[item_name]}")
        
    # Show row.data
    print(f"   Row 0 as dictionary (row.data):")
    row_data = first_row.data
    for item_name, value in list(row_data.items())[:3]:
        print(f"     {item_name}: {value}")
    if len(row_data) > 3:
        print(f"     ... and {len(row_data) - 3} more items")
    
    # Show row slicing
    if demo_category.row_count >= 3:
        print(f"\n📑 Row slicing example:")
        rows = demo_category[0:3]
        print(f"   Slicing category[0:3] returns {len(rows)} rows")
        for i, row in enumerate(rows):
            print(f"   Row {i}: {item_names[0]}={row[item_names[0]]}, {item_names[1]}={row[item_names[1]]}")
    
    print("\n💡 Usage Tips:")
    print("   1. Use category['item_name'] for column access")
    print("   2. Use category[index] for single row access")
    print("   3. Access row values with row.item_name or row['item_name']")
    print("   4. Get all row values as a dictionary with row.data")
    print("   5. Slice multiple rows with category[start:end]")

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
        data_container = handler.parse(args.input, categories=args.categories)
        
        # Show file information
        show_file_info(data_container)
        
        # Setup validation if requested
        if args.validate and data_container.data:
            print(f"\n🛡️  Setting up validation...")
            block = data_container.data[0]
            
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
        demonstrate_2d_slicing(data_container)
        
        # Modify data
        print(f"\n✏️  Modifying data...")
        modify_data(data_container)
        
        # Write output
        print(f"\n💾 Writing to: {args.output}")
        with open(args.output, 'w') as f:
            handler.file_obj = f
            handler.write(data_container)
        
        print(f"✅ Successfully processed!")
        
        # Verify the output
        print(f"\n🔍 Verifying output...")
        verify_data = handler.parse(args.output)
        print(f"✅ Output file contains {len(verify_data.data)} data block(s)")
        
        # Demonstrate 2D slicing if available
        if hasattr(handler, 'demonstrate_2d_slicing'):
            demonstrate_2d_slicing(data_container)
        
        # Clean up demo file if created
        if args.demo and os.path.exists("demo_structure.cif"):
            os.remove("demo_structure.cif")
            print("🧹 Cleaned up demo files")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
