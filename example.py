from mmcif_tools import MMCIFHandler, ValidatorFactory

# Example validators and cross-checkers
def category_validator(category_name):
    print(f"\nValidating category: {category_name}")

def cross_checker(category_name_1, category_name_2):
    print(f"\nCross-checking categories: {category_name_1} and {category_name_2}")

# Initialize the ValidatorFactory and register validators
validator_factory = ValidatorFactory()

# Initialize the handler
handler = MMCIFHandler(validator_factory=validator_factory)

# Parse the file with specific categories
content = handler.read('/Users/lucas/Desktop/em/emd_33233.cif', categories=['_database_2', '_atom_site'])

# Print data blocks
print(f"\nData blocks: {content.blocks}")

# Print file content
print(f"\nFile content: {content.data}")

# Accessing the DataBlock named '7XJP'
print(f"\nData Block: {content['7XJP']}")

# Accessing the first DataBlock in the file
print(f"\nFirst Data Block: {content.data[0]}")

category = content['7XJP']['_database_2']
other_category = content['7XJP']['_atom_site']

# Register the validators and cross-checkers
validator_factory.register_validator(category.name, category_validator)
validator_factory.register_cross_checker((category.name, other_category.name), cross_checker)

# Validate the category
category.validate()

# Cross-validate the categories
category.validate.against(other_category)

# Print each category and some of its values
for data_block in content:
    for category in data_block:
        print(f"\nCategory: {category}")
        print(f"  Name: {category.name}")
        print(f"  Items: {category.items}")
        for item, values in category.items.items():
            print(f"  Item: {item}, Values: {values[:5]}")

# Apply the modification
category.items['database_id'][-1] = 'NEWDB'

# Write the updated content back to the mmCIF file
with open('/Users/lucas/Desktop/em/modified_emd_33233.cif', 'w') as f:
    handler.file_obj = f
    handler.write()

# Verify the changes by re-reading the modified file
modified_handler = MMCIFHandler(validator_factory=validator_factory)
new_content = modified_handler.read('/Users/lucas/Desktop/em/modified_emd_33233.cif', categories=['_database_2', '_atom_site'])

for data_block in new_content:
    for category in data_block:
        print(f"Category: {category}")
        print(f"  Name: {category.name}")
        print(f"  Items: {category.items}")
        for item, values in category.items.items():
            print(f"  Item: {item}, Values: {values[:5]}")
