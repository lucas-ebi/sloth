import argparse
from mmcif_tools import MMCIFHandler, ValidatorFactory

# Example validators and cross-checkers
def category_validator(category_name):
    print(f"\nValidating category: {category_name}")

def cross_checker(category_name_1, category_name_2):
    print(f"\nCross-checking categories: {category_name_1} and {category_name_2}")

def modify_data(file):
    block = file.blocks[0]
    if '_database_2' in block:
        category = block['_database_2']
        if 'database_id' in category._items:
            category._items['database_id'][-1] = 'NEWDB'
            print("\nModified '_database_2.database_id' to 'NEWDB'")
        else:
            print("No 'database_id' found in '_database_2'")
    else:
        print("No '_database_2' category found")

def main():
    parser = argparse.ArgumentParser(
        description="mmCIF CLI tool for parsing, validating, modifying, and writing files.",
        epilog="Example usage:\n  python example.py emd_33233.cif emd_33233-modified.cif --categories _database_2 _atom_site --validate",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("input", help="Path to input mmCIF file")
    parser.add_argument("output", help="Path to write modified mmCIF file")
    parser.add_argument("--categories", nargs="+", help="List of categories to include", default=None)
    parser.add_argument("--validate", action="store_true", help="Run validation on selected categories")
    args = parser.parse_args()

    # Setup
    validator_factory = ValidatorFactory()
    handler = MMCIFHandler(atoms=True, validator_factory=validator_factory)

    # Parse input
    print(f"\nParsing: {args.input}")
    file = handler.parse(args.input, categories=args.categories)

    print(f"\nParsed Data Blocks: {file.blocks}")

    # Optionally validate
    if args.validate:
        block = file.blocks[0]
        if '_database_2' in block and '_atom_site' in block:
            cat = block['_database_2']
            other_cat = block['_atom_site']
            validator_factory.register_validator(cat.name, category_validator)
            validator_factory.register_cross_checker((cat.name, other_cat.name), cross_checker)
            cat.validate()
            cat.validate.against(other_cat)
        else:
            print("Skipping validation — required categories missing.")

    # Modify
    modify_data(file)

    # Write to output
    print(f"\nWriting modified file to: {args.output}")
    with open(args.output, 'w') as f:
        handler.file_obj = f
        handler.write(file)

    # Confirm
    print("\nVerifying output...")
    modified_file = handler.parse(args.output)
    for block in modified_file:
        for category in block:
            print(f"Category: {category.name}")
            for item, values in category:
                print(f"  Item: {item}, Values: {values[:5]}")

if __name__ == "__main__":
    main()
