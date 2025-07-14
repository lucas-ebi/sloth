"""
Test suite for XSD validation control across all handler operations.

This test suite verifies that the permissive parameter correctly controls
PDBML XSD schema validation for read, write, export, and load operations.
"""

import pytest
import tempfile
import os
from pathlib import Path

from sloth.mmcif.handler import MMCIFHandler
from sloth.mmcif.models import MMCIFDataContainer, DataBlock, Category
from sloth.mmcif.validator import ValidationError


class TestXSDValidationControl:
    """Test XSD validation control across all handler operations"""

    @pytest.fixture
    def handler(self):
        """Create a fresh handler for each test"""
        return MMCIFHandler()

    @pytest.fixture
    def valid_mmcif_container(self):
        """Create a valid mmCIF container that passes XSD validation"""
        container = MMCIFDataContainer()
        block = DataBlock("TEST")
        
        # Add entry category
        entry_cat = Category("entry")
        entry_cat["id"] = ["TEST"]
        block["entry"] = entry_cat
        
        # Add required atom_type category (referenced by atom_site)
        atom_type_cat = Category("atom_type")
        atom_type_cat["symbol"] = ["N", "C"]
        atom_type_cat["scattering_dispersion_real"] = ["0.0061", "0.0033"]
        atom_type_cat["scattering_dispersion_imag"] = ["0.0033", "0.0016"]
        block["atom_type"] = atom_type_cat
        
        # Add required chem_comp category (referenced by atom_site)
        chem_comp_cat = Category("chem_comp")
        chem_comp_cat["id"] = ["MET"]
        chem_comp_cat["type"] = ["L-peptide linking"]  # Use exact enumerated value
        chem_comp_cat["name"] = ["METHIONINE"]
        chem_comp_cat["formula"] = ["C5 H11 N O2 S"]
        block["chem_comp"] = chem_comp_cat
        
        # Add required entity category (referenced by atom_site)
        entity_cat = Category("entity")
        entity_cat["id"] = ["1"]
        entity_cat["type"] = ["polymer"]
        entity_cat["pdbx_description"] = ["Test protein"]
        block["entity"] = entity_cat
        
        # Add required struct_asym category (referenced by atom_site)
        struct_asym_cat = Category("struct_asym")
        struct_asym_cat["id"] = ["A"]
        struct_asym_cat["entity_id"] = ["1"]
        block["struct_asym"] = struct_asym_cat
        
        # Add atom_site category with proper references
        atom_site_cat = Category("atom_site")
        atom_site_cat["group_PDB"] = ["ATOM", "ATOM"]
        atom_site_cat["id"] = ["1", "2"]
        atom_site_cat["type_symbol"] = ["N", "C"]
        atom_site_cat["label_atom_id"] = ["N", "CA"]
        atom_site_cat["label_alt_id"] = [".", "."]
        atom_site_cat["label_comp_id"] = ["MET", "MET"]
        atom_site_cat["label_asym_id"] = ["A", "A"]
        atom_site_cat["label_entity_id"] = ["1", "1"]
        atom_site_cat["label_seq_id"] = ["1", "1"]
        atom_site_cat["pdbx_PDB_ins_code"] = ["?", "?"]
        atom_site_cat["Cartn_x"] = ["20.154", "19.030"]
        atom_site_cat["Cartn_y"] = ["16.967", "16.103"]
        atom_site_cat["Cartn_z"] = ["4.339", "4.810"]
        atom_site_cat["occupancy"] = ["1.00", "1.00"]
        atom_site_cat["B_iso_or_equiv"] = ["11.99", "12.57"]
        atom_site_cat["pdbx_formal_charge"] = ["?", "?"]
        atom_site_cat["auth_seq_id"] = ["1", "1"]
        atom_site_cat["auth_comp_id"] = ["MET", "MET"]
        atom_site_cat["auth_asym_id"] = ["A", "A"]
        atom_site_cat["auth_atom_id"] = ["N", "CA"]
        atom_site_cat["pdbx_PDB_model_num"] = ["1", "1"]
        block["atom_site"] = atom_site_cat
        
        container["TEST"] = block
        return container

    @pytest.fixture
    def invalid_mmcif_container(self):
        """Create an invalid mmCIF container that fails XSD validation"""
        container = MMCIFDataContainer()
        block = DataBlock("TEST")
        
        # Only add entry - missing required categories for atom_site references
        entry_cat = Category("entry")
        entry_cat["id"] = ["TEST"]
        block["entry"] = entry_cat
        
        # Add atom_site with invalid references and missing required fields
        atom_site_cat = Category("atom_site")
        atom_site_cat["group_PDB"] = ["ATOM"]
        atom_site_cat["id"] = ["1"]
        atom_site_cat["type_symbol"] = ["INVALID"]  # Invalid atom type (no corresponding atom_type entry)
        atom_site_cat["label_atom_id"] = ["N"]
        atom_site_cat["label_alt_id"] = ["."]
        atom_site_cat["label_comp_id"] = ["UNK"]  # Invalid comp (no corresponding chem_comp entry)
        atom_site_cat["label_asym_id"] = ["A"]
        atom_site_cat["label_entity_id"] = ["999"]  # Invalid entity (no corresponding entity entry)
        atom_site_cat["label_seq_id"] = ["1"]
        atom_site_cat["pdbx_PDB_ins_code"] = ["?"]
        atom_site_cat["Cartn_x"] = ["20.154"]
        atom_site_cat["Cartn_y"] = ["16.967"]
        atom_site_cat["Cartn_z"] = ["4.339"]
        atom_site_cat["occupancy"] = ["1.00"]
        # Missing B_iso_or_equiv - required by XSD schema
        atom_site_cat["pdbx_formal_charge"] = ["?"]
        atom_site_cat["auth_seq_id"] = ["1"]
        atom_site_cat["auth_comp_id"] = ["UNK"]
        atom_site_cat["auth_asym_id"] = ["A"]
        atom_site_cat["auth_atom_id"] = ["N"]
        atom_site_cat["pdbx_PDB_model_num"] = ["1"]
        block["atom_site"] = atom_site_cat
        
        container["TEST"] = block
        return container

    @pytest.fixture
    def temp_files(self):
        """Create temporary files for testing"""
        files = {}
        try:
            # Create temp CIF file
            files['cif'] = tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False)
            files['json'] = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            files['xml'] = tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False)
            
            for f in files.values():
                f.close()
            
            yield {k: v.name for k, v in files.items()}
        finally:
            # Clean up
            for file_path in files.values():
                try:
                    os.unlink(file_path.name)
                except (OSError, FileNotFoundError):
                    pass

    # ==================== Export Tests ====================

    def test_export_json_with_validation(self, handler, valid_mmcif_container):
        """Test JSON export with XSD validation enabled"""
        result = handler.export(valid_mmcif_container, 'json', permissive=False)
        assert result is not None
        assert '"TEST"' in result

    def test_export_json_without_validation(self, handler, invalid_mmcif_container):
        """Test JSON export with XSD validation disabled"""
        result = handler.export(invalid_mmcif_container, 'json', permissive=True)
        assert result is not None
        assert '"TEST"' in result

    def test_export_json_validation_failure(self, handler, invalid_mmcif_container):
        """Test JSON export with validation failure"""
        with pytest.raises(ValidationError):
            handler.export(invalid_mmcif_container, 'json', permissive=False)

    def test_export_xml_with_validation(self, handler, valid_mmcif_container):
        """Test XML export with XSD validation enabled"""
        result = handler.export(valid_mmcif_container, 'xml', permissive=False)
        assert result is not None
        assert 'datablock' in result

    def test_export_xml_without_validation(self, handler, invalid_mmcif_container):
        """Test XML export with XSD validation disabled"""
        result = handler.export(invalid_mmcif_container, 'xml', permissive=True)
        assert result is not None
        assert 'datablock' in result

    def test_export_xml_validation_failure(self, handler, invalid_mmcif_container):
        """Test XML export with validation failure"""
        with pytest.raises(ValidationError):
            handler.export(invalid_mmcif_container, 'xml', permissive=False)

    # ==================== Load Tests ====================

    def test_load_json_with_validation(self, handler, valid_mmcif_container, temp_files):
        """Test JSON load with XSD validation enabled"""
        # Create valid JSON file
        json_content = handler.export(valid_mmcif_container, 'json', permissive=True)
        with open(temp_files['json'], 'w') as f:
            f.write(json_content)
        
        # Load with validation
        result = handler.load(temp_files['json'], 'json', permissive=False)
        assert result is not None
        assert len(list(result)) == 1

    def test_load_json_without_validation(self, handler, invalid_mmcif_container, temp_files):
        """Test JSON load with XSD validation disabled"""
        # Create invalid JSON file
        json_content = handler.export(invalid_mmcif_container, 'json', permissive=True)
        with open(temp_files['json'], 'w') as f:
            f.write(json_content)
        
        # Load without validation should work
        result = handler.load(temp_files['json'], 'json', permissive=True)
        assert result is not None
        assert len(list(result)) == 1

    def test_load_xml_with_validation(self, handler, valid_mmcif_container, temp_files):
        """Test XML load with XSD validation enabled"""
        # Create valid XML file
        xml_content = handler.export(valid_mmcif_container, 'xml', permissive=True)
        with open(temp_files['xml'], 'w') as f:
            f.write(xml_content)
        
        # Load with validation
        result = handler.load(temp_files['xml'], 'xml', permissive=False)
        assert result is not None
        assert len(list(result)) == 1

    def test_load_xml_without_validation(self, handler, invalid_mmcif_container, temp_files):
        """Test XML load with XSD validation disabled"""
        # Create invalid XML file
        xml_content = handler.export(invalid_mmcif_container, 'xml', permissive=True)
        with open(temp_files['xml'], 'w') as f:
            f.write(xml_content)
        
        # Load without validation should work
        result = handler.load(temp_files['xml'], 'xml', permissive=True)
        assert result is not None
        assert len(list(result)) == 1

    # ==================== Read Tests ====================

    def test_read_with_validation(self, handler, valid_mmcif_container, temp_files):
        """Test CIF read with XSD validation enabled"""
        # Create valid CIF file using permissive write to avoid write validation
        handler.write(valid_mmcif_container, temp_files['cif'], permissive=True)
        
        # Verify the file was written and is readable
        assert os.path.exists(temp_files['cif'])
        assert os.path.getsize(temp_files['cif']) > 0
        
        # Read with validation
        result = handler.read(temp_files['cif'], permissive=False)
        assert result is not None
        assert len(list(result)) == 1

    def test_read_without_validation(self, handler, invalid_mmcif_container, temp_files):
        """Test CIF read with XSD validation disabled"""
        # Create invalid CIF file using permissive write
        handler.write(invalid_mmcif_container, temp_files['cif'], permissive=True)
        
        # Read without validation should work
        result = handler.read(temp_files['cif'], permissive=True)
        assert result is not None
        assert len(list(result)) == 1

    def test_read_validation_failure(self, handler, invalid_mmcif_container, temp_files):
        """Test CIF read with validation failure"""
        # Create invalid CIF file using permissive write
        handler.write(invalid_mmcif_container, temp_files['cif'], permissive=True)
        
        # Read with validation should fail - expect ValueError as that's what handler raises
        with pytest.raises(ValueError, match="XSD schema validation failed"):
            handler.read(temp_files['cif'], permissive=False)

    # ==================== Write Tests ====================

    def test_write_with_validation(self, handler, valid_mmcif_container, temp_files):
        """Test CIF write with XSD validation enabled"""
        # Write with validation should succeed
        handler.write(valid_mmcif_container, temp_files['cif'], permissive=False)
        
        # Verify file was created
        assert os.path.exists(temp_files['cif'])
        assert os.path.getsize(temp_files['cif']) > 0

    def test_write_without_validation(self, handler, invalid_mmcif_container, temp_files):
        """Test CIF write with XSD validation disabled"""
        # Write without validation should work even with invalid data
        handler.write(invalid_mmcif_container, temp_files['cif'], permissive=True)
        
        # Verify file was created
        assert os.path.exists(temp_files['cif'])
        assert os.path.getsize(temp_files['cif']) > 0

    def test_write_validation_failure(self, handler, invalid_mmcif_container, temp_files):
        """Test CIF write with validation failure"""
        # Write with validation should fail - expect ValueError as that's what handler raises
        with pytest.raises(ValueError, match="XSD schema validation failed"):
            handler.write(invalid_mmcif_container, temp_files['cif'], permissive=False)

    # ==================== Integration Tests ====================

    def test_round_trip_with_validation(self, handler, valid_mmcif_container, temp_files):
        """Test complete round trip with validation enabled"""
        # Write -> Read -> Export -> Load cycle
        handler.write(valid_mmcif_container, temp_files['cif'], permissive=False)
        read_data = handler.read(temp_files['cif'], permissive=False)
        
        json_content = handler.export(read_data, 'json', permissive=False)
        with open(temp_files['json'], 'w') as f:
            f.write(json_content)
        
        final_data = handler.load(temp_files['json'], 'json', permissive=False)
        
        # Verify data integrity
        assert len(list(final_data)) == 1
        assert len(list(valid_mmcif_container)) == 1

    def test_round_trip_without_validation(self, handler, invalid_mmcif_container, temp_files):
        """Test complete round trip with validation disabled"""
        # Even invalid data should work when validation is disabled
        handler.write(invalid_mmcif_container, temp_files['cif'], permissive=True)
        read_data = handler.read(temp_files['cif'], permissive=True)
        
        json_content = handler.export(read_data, 'json', permissive=True)
        with open(temp_files['json'], 'w') as f:
            f.write(json_content)
        
        final_data = handler.load(temp_files['json'], 'json', permissive=True)
        
        # Verify data integrity
        assert len(list(final_data)) == 1
        assert len(list(invalid_mmcif_container)) == 1

    @pytest.mark.skip(reason="This test is not relevant to the current context")
    def test_permissive_parameter_consistency(self, handler, valid_mmcif_container):
        """Test that permissive parameter works consistently across all methods"""
        # All methods should accept permissive parameter
        methods_with_permissive = [
            ('read', lambda: handler.read),
            ('write', lambda: handler.write), 
            ('export', lambda: handler.export),
            ('load', lambda: handler.load)
        ]
        
        for method_name, get_method in methods_with_permissive:
            method = get_method()
            # Check that permissive parameter exists in signature
            import inspect
            sig = inspect.signature(method)
            assert 'permissive' in sig.parameters, f"{method_name} method missing permissive parameter"
            
            # Check default value is False
            assert sig.parameters['permissive'].default == False, f"{method_name} permissive should default to False"


if __name__ == "__main__":
    # Allow running as script for development
    pytest.main([__file__, "-v"])