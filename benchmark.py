#!/usr/bin/env python3
"""
SLOTH Performance Benchmark Script

Tests parsing and memory usage across different file sizes to update the README performance matrix.
"""

import os
import time
import tempfile
import psutil
import sys
from typing import Dict, List, Tuple

from sloth import MMCIFHandler


def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # Convert bytes to MB


def create_test_file(size_kb: int, filename: str) -> str:
    """Create a test mmCIF file of approximately the given size."""
    
    # Base structure
    base_content = """data_TEST
_entry.id TEST_STRUCTURE
_database_2.database_id PDB
_database_2.database_code TEST
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.label_atom_id
_atom_site.label_asym_id
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
"""
    
    # Calculate how many atom records we need
    atom_line = "ATOM 1 C CA A 10.123 20.456 30.789\n"
    base_size = len(base_content)
    target_size = size_kb * 1024
    remaining_size = target_size - base_size
    
    if remaining_size <= 0:
        atom_count = 1
    else:
        atom_count = max(1, remaining_size // len(atom_line))
    
    # Generate atom records
    atoms = []
    for i in range(atom_count):
        x = 10.0 + (i % 100) * 0.1
        y = 20.0 + (i % 100) * 0.2
        z = 30.0 + (i % 100) * 0.3
        chain = chr(65 + (i % 26))  # A-Z
        atom_type = ["C", "N", "O", "S"][i % 4]
        atom_name = ["CA", "CB", "CG", "CD"][i % 4]
        atoms.append(f"ATOM {i+1} {atom_type} {atom_name} {chain} {x:.3f} {y:.3f} {z:.3f}")
    
    content = base_content + "\n".join(atoms) + "\n#\n"
    
    with open(filename, 'w') as f:
        f.write(content)
    
    actual_size = os.path.getsize(filename)
    return filename, actual_size


def benchmark_parsing(file_path: str, categories: List[str] = None) -> Dict:
    """Benchmark parsing performance for a given file."""
    handler = MMCIFHandler()
    
    # Measure memory before parsing
    initial_memory = get_memory_usage()
    
    # Time the parsing
    start_time = time.time()
    mmcif = handler.parse(file_path, categories=categories)
    parse_time = time.time() - start_time
    
    # Measure memory after parsing
    after_parse_memory = get_memory_usage()
    
    # Test access speed by accessing some data
    start_access = time.time()
    if mmcif.data and '_atom_site' in mmcif.data[0].categories:
        atom_site = mmcif.data[0]._atom_site
        if hasattr(atom_site, 'Cartn_x') and len(atom_site.Cartn_x) > 0:
            # Access first atom
            first_atom = atom_site[0]
            x_coord = first_atom.Cartn_x
            # Access some coordinates
            if len(atom_site.Cartn_x) > 10:
                coords = [atom_site.Cartn_x[i] for i in range(min(10, len(atom_site.Cartn_x)))]
    access_time = time.time() - start_access
    
    # Measure final memory usage
    final_memory = get_memory_usage()
    
    return {
        'parse_time': parse_time,
        'access_time': access_time,
        'memory_used': final_memory - initial_memory,
        'atom_count': len(mmcif.data[0]._atom_site.Cartn_x) if mmcif.data and '_atom_site' in mmcif.data[0].categories and hasattr(mmcif.data[0]._atom_site, 'Cartn_x') else 0,
        'categories': len(mmcif.data[0].categories) if mmcif.data else 0
    }


def format_time(seconds: float) -> str:
    """Format time in a human-readable way."""
    if seconds < 0.001:
        return f"{seconds*1000000:.0f}μs"
    elif seconds < 0.1:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 1:
        return f"{seconds*1000:.0f}ms"
    else:
        return f"{seconds:.1f}s"


def format_memory(mb: float) -> str:
    """Format memory in a human-readable way."""
    if mb < 1:
        return f"{mb*1024:.0f}KB"
    elif mb < 1024:
        return f"{mb:.1f}MB"
    else:
        return f"{mb/1024:.1f}GB"


def run_benchmarks():
    """Run comprehensive benchmarks and print results."""
    print("🦥 SLOTH Performance Benchmark")
    print("=" * 50)
    
    # Test cases: (size_kb, description)
    test_cases = [
        (1, "Small sample"),
        (10, "Tiny structure"),
        (100, "Small structure"),
        (1000, "Medium structure"),
        (10000, "Large structure"),
        (50000, "Very large structure")
    ]
    
    results = []
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for size_kb, description in test_cases:
            print(f"\n📊 Testing {description} (~{size_kb}KB)")
            
            # Create test file
            test_file = os.path.join(temp_dir, f"test_{size_kb}kb.cif")
            try:
                test_file, actual_size = create_test_file(size_kb, test_file)
                actual_size_kb = actual_size / 1024
                
                print(f"   Created: {actual_size_kb:.1f}KB")
                
                # Full parse benchmark
                full_result = benchmark_parsing(test_file)
                
                # Selective parse benchmark (atom_site only)
                selective_result = benchmark_parsing(test_file, categories=['_atom_site', '_entry'])
                
                results.append({
                    'size_kb': actual_size_kb,
                    'description': description,
                    'full_parse': full_result,
                    'selective_parse': selective_result
                })
                
                print(f"   Full parse: {format_time(full_result['parse_time'])}, Memory: {format_memory(full_result['memory_used'])}")
                print(f"   Selective:  {format_time(selective_result['parse_time'])}, Memory: {format_memory(selective_result['memory_used'])}")
                print(f"   Access:     {format_time(full_result['access_time'])}")
                print(f"   Atoms:      {full_result['atom_count']:,}")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                continue
    
    # Print summary table
    print("\n" + "=" * 80)
    print("📈 PERFORMANCE MATRIX")
    print("=" * 80)
    print(f"{'File Size':<12} {'Full Parse':<12} {'Selective':<12} {'Access':<10} {'Memory':<10} {'Description':<15}")
    print("-" * 80)
    
    for result in results:
        size_str = f"{result['size_kb']:.0f}KB" if result['size_kb'] < 1000 else f"{result['size_kb']/1000:.1f}MB"
        full_time = format_time(result['full_parse']['parse_time'])
        selective_time = format_time(result['selective_parse']['parse_time'])
        access_time = format_time(result['full_parse']['access_time'])
        memory = format_memory(result['full_parse']['memory_used'])
        
        print(f"{size_str:<12} {full_time:<12} {selective_time:<12} {access_time:<10} {memory:<10} {result['description']:<15}")
    
    # Generate markdown table for README
    print("\n" + "=" * 50)
    print("📝 MARKDOWN TABLE FOR README")
    print("=" * 50)
    print("| File Size     | Full Parse   | Selective Parse | Access Speed | Memory Usage | Example |")
    print("|---------------|--------------|-----------------|--------------|---------------|---------|")
    
    for result in results:
        size_range = ""
        if result['size_kb'] < 10:
            size_range = "<10KB"
        elif result['size_kb'] < 100:
            size_range = "10KB–100KB"
        elif result['size_kb'] < 1000:
            size_range = "100KB–1MB"
        elif result['size_kb'] < 10000:
            size_range = "1MB–10MB"
        elif result['size_kb'] < 100000:
            size_range = "10MB–100MB"
        else:
            size_range = ">100MB"
        
        full_time = format_time(result['full_parse']['parse_time'])
        selective_time = format_time(result['selective_parse']['parse_time'])
        access_time = format_time(result['full_parse']['access_time'])
        memory = format_memory(result['full_parse']['memory_used'])
        
        print(f"| {size_range:<13} | {full_time:<12} | {selective_time:<15} | {access_time:<12} | {memory:<13} | {result['description']} |")


if __name__ == "__main__":
    # Check dependencies
    try:
        import psutil
    except ImportError:
        print("❌ psutil not found. Installing...")
        os.system("pip install psutil")
        import psutil
    
    run_benchmarks()
