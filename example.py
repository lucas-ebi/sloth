from mmcif_tools import MMCIFHandler, ValidatorFactory

# Example validators and cross-checkers
def category_validator(category_name):
    print(f"\nValidating category: {category_name}")

def cross_checker(category_name_1, category_name_2):
    print(f"\nCross-checking categories: {category_name_1} and {category_name_2}")
    
def modify_data(modified_file):
    # Assuming there is a data block named ''
    data_block = modified_file.data_

    # Assuming there is a category named '_database_2'
    category = data_block._database_2

    # Print category.database_id
    print(f"\nCategory.database_id: {category.database_id}")

    # Print the content of category.database_id as a list
    print(f"\nCategory.database_id as list: {list(category.database_id)}")

    # # Modify an item in the category
    # database_ids = list(category.database_id)
    # database_ids[-1] = 'NEWDB'
    # category.database_id = iter(database_ids)

# Initialize the ValidatorFactory and register validators
validator_factory = ValidatorFactory()

# Initialize the handler
handler = MMCIFHandler(atoms=True, validator_factory=validator_factory)

# Parse the file with specific categories
file = handler.parse('/Users/lucas/Desktop/em/example.cif')#, categories=['_database_2', '_atom_site'])

# Print data blocks
print(f"\nData blocks: {file.blocks}")

# Print file content
print(f"\nFile content: {file.data}")

# Accessing the DataBlock named ''
print(f"\nData Block: {file.data_}")

# Accessing the first DataBlock in the file
print(f"\nFirst Data Block: {file.data[0]}")

category = file.data_._database_2
other_category = file.data_._atom_site

# Register the validators and cross-checkers
validator_factory.register_validator(category.name, category_validator)
validator_factory.register_cross_checker((category.name, other_category.name), cross_checker)

# Validate the category
category.validate()

# Cross-validate the categories
category.validate.against(other_category)

for data_block in file:
    for category in data_block:
        print(f"\nCategory: {category}")
        print(f"  Name: {category.name}")
        print(f"  Items: {category.items}")
        for item, value in category.data.items():
            print(f"    Item: {item}, Value: {value}")

# Apply the modification
modify_data(file)

# Write the updated content back to a new mmCIF file
with open('/Users/lucas/Desktop/em/modified_example.cif', 'w') as f:
    handler.file_obj = f
    handler.write(file)

# Verify the changes
modified_file = handler.parse('/Users/lucas/Desktop/em/modified_example.cif')#, categories=['_database_2', '_atom_site'])
for data_block in modified_file:
    for category in data_block:
        print(f"\nCategory: {category}")
        print(f"  Name: {category.name}")
        print(f"    Items: {category.items}")
        for item in category:
            # print(f"      Item: {item}, Values: {[value for value in values][:5]}")
            print(f"      Item: {item.name}, Values: {item.values[:5]}")
