from mmcif_tools import MMCIFHandler

# Parse the CIF file
handler = MMCIFHandler()
file = handler.parse("/Users/lucas/Desktop/em/emd_33233_md.cif")

# Accessing the DataBlock named '7XJP'
data = getattr(file, '7XJP')

# Access the '_database_2' category and its items using dot-separated notation
data._database_2.items

data._database_2.database_id

data._database_2.database_code

data._database_2.pdbx_database_accession

data._database_2.pdbx_DOI

# Write a copy of the mmCIF file
handler.write("/Users/lucas/Desktop/em/test_emd_33233_md.cif", file)

# # Update values in an item
# data._database_2.database_id[-1] = 'NEWDB'
# data._database_2.database_id

# # Write updated content in mmCIF format
# handler.write("/Users/lucas/Desktop/em/modified_emd_33233_md.cif", file)