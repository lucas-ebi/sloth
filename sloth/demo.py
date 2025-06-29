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


def modify_data(mmcif_data_container):
    """Example data modification."""
    if not mmcif_data_container.data:
        print("❌ No data blocks found")
        return
    
    block = mmcif_data_container.data[0]
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


def show_file_info(mmcif_data_container):
    """Display information about the parsed file."""
    print(f"\n📊 File Information:")
    print(f"   Data blocks: {len(mmcif_data_container.data)}")
    
    for i, block in enumerate(mmcif_data_container.data):
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

def demonstrate_2d_slicing(mmcif_data_container):
    """Demonstrate 2D slicing functionality with emphasis on dot notation."""
    if not mmcif_data_container.data:
        print("❌ No data blocks found")
        return
    
    block = mmcif_data_container.data[0]
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
        mmcif_data_container = handler.parse(args.input, categories=args.categories)
        
        # Show file information
        show_file_info(mmcif_data_container)
        
        # Setup validation if requested
        if args.validate and mmcif_data_container.data:
            print(f"\n🛡️  Setting up validation...")
            block = mmcif_data_container.data[0]
            
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
        demonstrate_2d_slicing(mmcif_data_container)
        
        # Modify data
        print(f"\n✏️  Modifying data...")
        modify_data(mmcif_data_container)
        
        # Write output
        print(f"\n💾 Writing to: {args.output}")
        with open(args.output, 'w') as f:
            handler.file_obj = f
            handler.write(mmcif_data_container)
        
        print(f"✅ Successfully processed!")
        
        # Verify the output
        print(f"\n🔍 Verifying output...")
        verify_data = handler.parse(args.output)
        print(f"✅ Output file contains {len(verify_data.data)} data block(s)")
        
        # Demonstrate 2D slicing if available
        if hasattr(handler, 'demonstrate_2d_slicing'):
            demonstrate_2d_slicing(mmcif_data_container)
        
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
