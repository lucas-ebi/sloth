#!/usr/bin/env python3
"""
Development utilities for the Sloth package.
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(e.stderr)
        return False

def build_package():
    """Build the package."""
    return run_command("python -m build", "Building package")

def run_tests():
    """Run tests."""
    return run_command("python -m pytest tests/ -v", "Running tests")

def install_dev():
    """Install in development mode."""
    return run_command("pip install -e .[dev]", "Installing in development mode")

def check_package():
    """Check the package with twine."""
    return run_command("twine check dist/*", "Checking package")

def clean():
    """Clean build artifacts."""
    dirs_to_remove = ["build/", "dist/", "*.egg-info/", "__pycache__/"]
    for pattern in dirs_to_remove:
        run_command(f"rm -rf {pattern}", f"Cleaning {pattern}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python dev.py [build|test|install|check|clean|all]")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "build":
        build_package()
    elif command == "test":
        run_tests()
    elif command == "install":
        install_dev()
    elif command == "check":
        check_package()
    elif command == "clean":
        clean()
    elif command == "all":
        clean()
        install_dev()
        run_tests()
        build_package()
        check_package()
        print("🎉 All tasks completed!")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()
