from mmcif_tools import MMCIFHandler

def modify_data(modified_file):
    # Assuming there is a data block named '7XJP'
    data_block = modified_file['7XJP']  # Replace '7XJP' with the actual block name

    # Assuming there is a category named '_database_2'
    category = data_block['_database_2']  # Replace '_database_2' with the actual category name

    # Modify an item in the category
    category._items['database_id'][-1] = 'NEWDB'

# Initialize the handler
handler = MMCIFHandler(atoms=False, validator_factory=None)

# Parse the file with specific categories
file = handler.parse('/Users/lucas/Desktop/em/emd_33233.cif', categories=['_database_2', '_atom_site'])

# Print data blocks
print(f"Data blocks: {file.data}")

# data = getattr(file, '7XJP')  # Accessing the DataBlock named '7XJP'
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
