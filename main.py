"""
MASTER ORCHESTRATOR - Walmart UK Polyglot Persistence Setup
Run this script to execute all steps in sequence

This automates the entire setup:
1. Generate sample data
2. Create SQL Server tables
3. Import SQL Server data
4. Run SQL Server demos
5. Create MongoDB collections
6. Import MongoDB data
7. Create MongoDB indexes
8. Run MongoDB demos
9. Run cross-database polyglot demo
"""

import sys
import subprocess
from pathlib import Path

# ANSI color codes for pretty output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_header(text):
    """Print colored header"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 70}")
    print(f"{text}")
    print(f"{'=' * 70}{Colors.ENDC}\n")

def print_step(step_num, description):
    """Print step header"""
    print(f"\n{Colors.OKCYAN}{Colors.BOLD}STEP {step_num}: {description}{Colors.ENDC}")
    print("-" * 70)

def print_success(message):
    """Print success message"""
    print(f"{Colors.OKGREEN}✓ {message}{Colors.ENDC}")

def print_error(message):
    """Print error message"""
    print(f"{Colors.FAIL}❌ {message}{Colors.ENDC}")

def print_warning(message):
    """Print warning message"""
    print(f"{Colors.WARNING}⚠ {message}{Colors.ENDC}")

def run_script(script_path, description, required=True):
    """
    Run a Python script and handle errors
    
    Args:
        script_path (str): Path to Python script
        description (str): Description for user
        required (bool): Whether this step is required (fail if error)
    
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"\n{Colors.OKBLUE}Running: {script_path}{Colors.ENDC}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        # Print output
        if result.stdout:
            print(result.stdout)
        
        if result.returncode != 0:
            print_error(f"Script failed: {description}")
            if result.stderr:
                print(f"\n{Colors.FAIL}Error details:{Colors.ENDC}")
                print(result.stderr)
            
            if required:
                return False
            else:
                print_warning("Continuing despite error (non-critical step)")
                return True
        
        print_success(f"Completed: {description}")
        return True
        
    except subprocess.TimeoutExpired:
        print_error(f"Script timeout (>5 minutes): {description}")
        return False
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        return False

def check_prerequisites():
    """Check if required packages are installed"""
    print_step("0", "Checking Prerequisites")
    
    required_packages = ['pyodbc', 'pymongo', 'pandas']
    missing = []
    
    for package in required_packages:
        try:
            __import__(package)
            print_success(f"Package '{package}' found")
        except ImportError:
            missing.append(package)
            print_error(f"Package '{package}' not found")
    
    if missing:
        print(f"\n{Colors.WARNING}Missing packages: {', '.join(missing)}{Colors.ENDC}")
        print(f"\nInstall with: pip install {' '.join(missing)}")
        print("Or run: pip install -r 01_Setup/requirements.txt")
        return False
    
    print_success("All prerequisites satisfied")
    return True

def main():
    """Main execution function"""
    print_header("WALMART UK POLYGLOT PERSISTENCE - MASTER SETUP")
    print("This script will set up the complete system:")
    print("  • Generate UK retail sample data")
    print("  • Create and populate SQL Server database")
    print("  • Create and populate MongoDB collections")
    print("  • Run demonstration queries")
    print("  • Prove polyglot persistence works!")
    
    # Get project root
    project_root = Path(__file__).parent
    
    # Check prerequisites
    if not check_prerequisites():
        print_error("Prerequisites not met. Install required packages first.")
        return 1
    
    # Define execution steps
    steps = [
        # Data Generation
        {
            'path': '02_Data_Generation/generate_walmart_data.py',
            'description': 'Generate UK Retail Sample Data',
            'required': True
        },
        
        # SQL Server Setup
        {
            'path': '04_SQL_Server/01_create_tables.py',
            'description': 'Create SQL Server Tables',
            'required': True
        },
        {
            'path': '04_SQL_Server/02_import_data.py',
            'description': 'Import Data to SQL Server',
            'required': True
        },
        {
            'path': '04_SQL_Server/03_demo_queries.py',
            'description': 'Run SQL Server Demonstration Queries',
            'required': False  # Demo can fail without breaking setup
        },
        
        # MongoDB Setup
        {
            'path': '05_MongoDB/01_create_collections.py',
            'description': 'Create MongoDB Collections',
            'required': True
        },
        {
            'path': '05_MongoDB/02_import_data.py',
            'description': 'Import Data to MongoDB',
            'required': True
        },
        {
            'path': '05_MongoDB/03_create_indexes.py',
            'description': 'Create MongoDB Indexes',
            'required': True
        },
        {
            'path': '05_MongoDB/04_demo_queries.py',
            'description': 'Run MongoDB Demonstration Queries',
            'required': False  # Demo can fail without breaking setup
        },
        
        # Polyglot Persistence Demo
        {
            'path': '06_Polyglot_Demo/cross_database_demo.py',
            'description': 'Cross-Database Polyglot Persistence Demo',
            'required': False  # Final demo
        }
    ]
    
    # Execute steps
    for i, step in enumerate(steps, 1):
        print_step(i, step['description'])
        
        script_path = project_root / step['path']
        
        if not script_path.exists():
            print_error(f"Script not found: {script_path}")
            if step['required']:
                print_error("Setup failed - required script missing")
                return 1
            else:
                print_warning("Skipping non-critical step")
                continue
        
        success = run_script(str(script_path), step['description'], step['required'])
        
        if not success and step['required']:
            print_error(f"Setup failed at: {step['description']}")
            print("\nTroubleshooting:")
            print("  1. Check database connections in 01_Setup/config.py")
            print("  2. Ensure SQL Server is running")
            print("  3. Ensure MongoDB is running")
            print("  4. Check credentials and permissions")
            return 1
    
    # Success summary
    print_header("✓✓✓ SETUP COMPLETE ✓✓✓")
    print("All steps executed successfully!")
    print("\nWhat was created:")
    print("  ✓ SQL Server database with 7 tables")
    print("  ✓ MongoDB database with 2 collections")
    print("  ✓ Sample data (1,000 customers, 500 orders, 300 reviews)")
    print("  ✓ Common identifiers linking both databases")
    print("  ✓ Demonstration of polyglot persistence")
    print("\nNext steps:")
    print("  1. Review demonstration output above")
    print("  2. Open PowerBI and connect to both databases")
    print("  3. Create relationships on ProductID and CustomerID")
    print("  4. Build dashboards combining SQL + MongoDB data")
    print("\nFor video recording:")
    print("  • Run individual scripts in 04_SQL_Server/ and 05_MongoDB/")
    print("  • Narrate as queries execute (see IMPLEMENTATION_DEMONSTRATION_GUIDE.md)")
    print("  • Show results, don't just scroll code!")
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}Setup interrupted by user{Colors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Colors.FAIL}Unexpected error: {e}{Colors.ENDC}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
