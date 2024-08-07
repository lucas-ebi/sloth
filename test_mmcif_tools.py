from mmcif_tools import MMCIFFileReader

reader = MMCIFFileReader(atoms=False, validator_factory=None)
data_container = reader.read('/Users/lucas/Desktop/em/emd_33233_md.cif')

# To verify the contents
for block_name, data_block in data_container.data_blocks.items():
    print(f"Data Block: {block_name}")
    for category_name, category in data_block.categories.items():
        print(f"Category: {category_name}")
        for item_name, values in category._items.items():
            print(f"  Item: {item_name}, Values: {values}")
