"""
App Code Generator E2E Test: Generate React + FastAPI application

Tests:
1. Generate app spec from use case
2. Generate React components
3. Generate FastAPI backend
4. Generate deployment configs
5. Save generated files
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

os.environ["SNOWFLAKE_CONNECTION_NAME"] = "my_snowflake"

DATABASE = "DRILLING_OPS_DB"
CONNECTION_NAME = "my_snowflake"


def test_generate_app():
    """Generate complete React + FastAPI app for drilling operations."""
    print("\n" + "=" * 60)
    print("APP CODE GENERATOR E2E TEST")
    print("=" * 60)
    
    from src.agents.app_generation.code_generator import AppCodeGenerator
    
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "generated_app"
    )
    
    generator = AppCodeGenerator(
        connection_name=CONNECTION_NAME,
        model="mistral-large2",
        output_dir=output_dir,
    )
    
    use_case = """
    Build a drilling operations dashboard with:
    - Stuck pipe prediction using ML model
    - Search historical drilling reports for incidents
    - Query drilling metrics through natural language
    - Visualize sensor data trends
    """
    
    tables = [
        f"{DATABASE}.RAW.DRILLING_TIME",
        f"{DATABASE}.RAW.DAILY_DRILLING_REPORTS",
    ]
    
    models = [f"{DATABASE}.ML.STUCK_PIPE_DETECTOR"]
    search_services = [f"{DATABASE}.CORTEX.DDR_SEARCH"]
    semantic_models = ["drilling_sensor_model.yaml"]
    
    print("\n📋 STEP 1: Generate App Specification")
    print("-" * 40)
    
    result = generator.generate(
        use_case=use_case,
        tables=tables,
        models=models,
        search_services=search_services,
        semantic_models=semantic_models,
    )
    
    if result.get("status") != "complete":
        print(f"❌ Generation failed: {result.get('error')}")
        return False
    
    spec = result.get("spec", {})
    print(f"✅ App Name: {spec.get('app_name')}")
    print(f"   Pages: {len(spec.get('pages', []))}")
    print(f"   Features: {spec.get('features')}")
    
    print("\n📁 STEP 2: Generated Files")
    print("-" * 40)
    
    files = result.get("files", {})
    
    file_categories = {
        "React Frontend": [],
        "FastAPI Backend": [],
        "Deployment": [],
    }
    
    for path in files.keys():
        if path.startswith("frontend/"):
            file_categories["React Frontend"].append(path)
        elif path.startswith("backend/"):
            file_categories["FastAPI Backend"].append(path)
        else:
            file_categories["Deployment"].append(path)
    
    for category, paths in file_categories.items():
        print(f"\n   {category}:")
        for path in paths:
            print(f"   - {path}")
    
    print(f"\n   Total files: {len(files)}")
    
    print("\n💾 STEP 3: Save Generated Files")
    print("-" * 40)
    
    os.makedirs(output_dir, exist_ok=True)
    
    saved_count = 0
    for path, content in files.items():
        full_path = os.path.join(output_dir, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        with open(full_path, 'w') as f:
            f.write(content)
        saved_count += 1
    
    print(f"✅ Saved {saved_count} files to: {output_dir}")
    
    print("\n📝 STEP 4: Sample Generated Code")
    print("-" * 40)
    
    if "frontend/src/App.tsx" in files:
        print("\n   App.tsx (preview):")
        for line in files["frontend/src/App.tsx"].split('\n')[:15]:
            print(f"   {line}")
        print("   ...")
    
    if "backend/main.py" in files:
        print("\n   main.py (preview):")
        for line in files["backend/main.py"].split('\n')[:15]:
            print(f"   {line}")
        print("   ...")
    
    print("\n" + "=" * 60)
    print("APP CODE GENERATOR SUMMARY")
    print("=" * 60)
    print(f"✅ App Name: {spec.get('app_name')}")
    print(f"✅ Pages: {len(spec.get('pages', []))}")
    print(f"✅ Total files generated: {len(files)}")
    print(f"✅ Output directory: {output_dir}")
    
    return True


def run_app_generator_test():
    """Run the app generator test."""
    results = {}
    
    try:
        results["generate_app"] = test_generate_app()
    except Exception as e:
        import traceback
        print(f"❌ Failed: {e}")
        traceback.print_exc()
        results["generate_app"] = False
    
    return results


if __name__ == "__main__":
    run_app_generator_test()
