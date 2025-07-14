#!/usr/bin/env python3
"""
Generate precomputed DBML relationship mappings for SLOTH.

This script generates DBML files that can be used for faster startup times
when using precomputed=True in the JSON exporter.

Usage:
    python -m sloth.mmcif.scripts.generate_dbml [options]
"""

import os
import sys
from pathlib import Path

try:
    from ..serializer import (
        PrecomputedMappingGenerator, DictionaryParser, XSDParser, get_cache_manager
    )
except ImportError:
    # This happens when script is run directly instead of as a module
    print("❌ This script should be run as a module:")
    print("   python -m sloth.mmcif.scripts.generate_dbml [options]")
    print("\nFor help:")
    print("   python -m sloth.mmcif.scripts.generate_dbml --help")
    sys.exit(1)


def generate_dbml_mappings(
    dict_path: str = None,
    xsd_path: str = None,
    output_dir: str = None,
    quiet: bool = False
):
    """Generate DBML mappings from dictionary and XSD files"""
    
    # Get the schemas directory
    schemas_dir = Path(__file__).parent.parent / "schemas"
    
    # Set default paths to the standard schema files
    if dict_path is None:
        dict_path = schemas_dir / "mmcif_pdbx_v50.dic"
    if xsd_path is None:
        xsd_path = schemas_dir / "pdbx-v50.xsd"
    if output_dir is None:
        output_dir = schemas_dir
    
    # Validate that the schema files exist
    if not Path(dict_path).exists():
        raise FileNotFoundError(f"mmCIF dictionary file not found: {dict_path}")
    if not Path(xsd_path).exists():
        raise FileNotFoundError(f"PDBML XSD schema file not found: {xsd_path}")
    
    if not quiet:
        print(f"📖 Using mmCIF dictionary: {Path(dict_path).name}")
        print(f"📋 Using PDBML XSD schema: {Path(xsd_path).name}")
        print(f"📁 Output directory: {output_dir}")
        print()
    
    # Set up components
    cache_manager = get_cache_manager()
    dict_parser = DictionaryParser(cache_manager, quiet)
    xsd_parser = XSDParser(cache_manager, quiet)
    dict_parser.source = dict_path
    xsd_parser.source = xsd_path
    
    # Create precomputed mapping generator
    mapping_generator = PrecomputedMappingGenerator(
        dict_parser, xsd_parser, cache_manager, quiet, 
        precomputed=False, schemas_dir=output_dir
    )
    
    # Force computation and export
    mapping_generator.get_mapping_rules()  # This will compute and cache
    dbml_path = mapping_generator.force_export_mappings()
    
    if not quiet:
        print(f"✅ Generated DBML mappings at: {dbml_path}")
        print(f"📊 File size: {dbml_path.stat().st_size} bytes")
        
        # Show usage example
        print(f"\n💡 Usage example:")
        print(f"   exporter = JSONExporter(precomputed=True)")
        print(f"   # Will use precomputed mappings from {dbml_path.name}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate precomputed DBML mappings")
    parser.add_argument("--dict", help="Path to mmCIF dictionary file (default: schemas/mmcif_pdbx_v50.dic)")
    parser.add_argument("--xsd", help="Path to PDBML XSD schema file (default: schemas/pdbx-v50.xsd)")
    parser.add_argument("--output", help="Output directory for DBML files (default: schemas/)")
    parser.add_argument("--quiet", action="store_true", help="Suppress output")
    
    args = parser.parse_args()
    
    generate_dbml_mappings(
        dict_path=args.dict,
        xsd_path=args.xsd,
        output_dir=args.output,
        quiet=args.quiet
    )