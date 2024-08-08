from mmcif_tools import MMCIFHandler, ValidatorFactory

# Example validators and cross-checkers
def database_validator(category_name):
    print(f"Validating category: {category_name}")

def cross_checker(category_name_1, category_name_2):
    print(f"Cross-checking categories: {category_name_1} and {category_name_2}")

# Initialize the ValidatorFactory and register validators
validator_factory = ValidatorFactory()
validator_factory.register_validator('_database_2', database_validator)
validator_factory.register_cross_checker(('_database_2', '_atom_site'), cross_checker)

def modify_data(modified_file):
    # Assuming there is a data block named '7XJP'
    data_block = modified_file['7XJP']  # Replace '7XJP' with the actual block name

    # Assuming there is a category named '_database_2'
    category = data_block['_database_2']  # Replace '_database_2' with the actual category name

    # Modify an item in the category
    category._items['database_id'][-1] = 'NEWDB'

    # Validate the category
    category.validate()

    # Assuming there is another category named '_atom_site'
    other_category = data_block['_atom_site']
    
    # Cross-validate the categories
    category.validate().against(other_category)

# Initialize the handler
handler = MMCIFHandler(atoms=True, validator_factory=validator_factory)

# Parse the file with specific categories
file = handler.parse('/Users/lucas/Desktop/em/emd_33233.cif', categories=['_database_2', '_atom_site'])

# Print file content
print(f"File: {file}")

# Print data blocks
print(f"Data blocks: {file.data}")

# Accessing the DataBlock named '7XJP'
print(f"Data Block: {file.data_7XJP}")

for data_block in file:
    print(f"Data Block: {data_block}")
    for category in data_block:
        print(f"Category: {category}")
        print(f"  Name: {category.name}")
        print(f"  Items: {category.items}")
        for item, values in category:
            print(f"  Item: {item}, Values: {values}")

# Apply the modification
modify_data(file)

# Write the updated content back to a new mmCIF file
with open('/Users/lucas/Desktop/em/modified_emd_33233.cif', 'w') as f:
    handler.file_obj = f
    handler.write(file)

# Verify the changes
modified_file = handler.parse('/Users/lucas/Desktop/em/modified_emd_33233.cif', categories=['_database_2', '_atom_site'])
for data_block in modified_file:
    print(f"Data Block: {data_block}")
    for category in data_block:
        print(f"Category: {category}")
        print(f"  Name: {category.name}")
        print(f"  Items: {category.items}")
        for item, values in category:
            print(f"  Item: {item}, Values: {values}")
