from mmcif_tools import MMCIFHandler

def modify_data(data_container):
    # Assuming there is a data block named '7XJP'
    data_block = data_container['7XJP']  # Replace '7XJP' with the actual block name

    # Assuming there is a category named '_database_2'
    category = data_block['_database_2']  # Replace '_database_2' with the actual category name

    # Modify an item in the category
    category._items['database_id'][-1] = 'NEWDB'

# Initialize the handler
handler = MMCIFHandler(atoms=True, validator_factory=None)

# Parse the file with specific categories
data_container = handler.parse('/Users/lucas/Desktop/em/emd_33233.cif', categories=['_database_2', '_atom_site'])

# Apply the modification
modify_data(data_container)

# Write the updated content back to a new mmCIF file
with open('/Users/lucas/Desktop/em/modified_emd_33233.cif', 'w') as f:
    handler.file_obj = f
    handler.write(data_container)

# Verify the changes
data_container = handler.parse('/Users/lucas/Desktop/em/modified_emd_33233.cif', categories=['_database_2', '_atom_site'])
for block_name, data_block in data_container.data_blocks.items():
    print(f"Data Block: {block_name}")
    for category_name, category in data_block.categories.items():
        print(f"Category: {category_name}")
        print(f"  Items: {category._items}")
        for item_name, values in category._items.items():
            print(f"  Item: {item_name}, Values: {values}")
