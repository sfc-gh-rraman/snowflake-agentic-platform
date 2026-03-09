"""
Semantic Model Generator E2E Test: Create semantic model for Cortex Analyst

Tests:
1. Generate semantic YAML for drilling sensor data
2. Generate semantic YAML for DDR reports
3. Validate generated YAML structure
4. Test with Cortex Analyst (if possible)
"""

import os
import sys
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

os.environ["SNOWFLAKE_CONNECTION_NAME"] = "my_snowflake"

DATABASE = "DRILLING_OPS_DB"
CORTEX_SCHEMA = "CORTEX"
RAW_SCHEMA = "RAW"
CONNECTION_NAME = "my_snowflake"


def test_generate_drilling_semantic_model():
    """Generate semantic model for DRILLING_TIME table."""
    print("\n" + "=" * 60)
    print("STEP 1: Generate Semantic Model for Drilling Sensor Data")
    print("=" * 60)
    
    from src.agents.semantic.model_generator import SemanticModelGenerator
    
    generator = SemanticModelGenerator(
        connection_name=CONNECTION_NAME,
        database=DATABASE,
        schema=CORTEX_SCHEMA,
        model="mistral-large2",
    )
    
    try:
        yaml_content = generator.generate_yaml(
            table_name=f"{DATABASE}.{RAW_SCHEMA}.DRILLING_TIME",
            model_name="DRILLING_SENSOR_MODEL",
            business_context="Oil and gas drilling operations sensor data including ROP, WOB, torque, pressure for drilling performance analysis",
            use_llm=True,
        )
        
        print(f"✅ Generated semantic model YAML ({len(yaml_content)} chars)")
        print("\n   Preview:")
        for line in yaml_content.split('\n')[:15]:
            print(f"   {line}")
        print("   ...")
        
        return True, generator, yaml_content
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False, None, None


def test_validate_yaml(yaml_content):
    """Validate the generated YAML structure."""
    print("\n" + "=" * 60)
    print("STEP 2: Validate YAML Structure")
    print("=" * 60)
    
    try:
        parsed = yaml.safe_load(yaml_content)
        
        checks = {
            "has_name": "name" in parsed,
            "has_tables": "tables" in parsed and len(parsed.get("tables", [])) > 0,
            "has_dimensions": any("dimensions" in t for t in parsed.get("tables", [])),
            "has_facts": any("facts" in t for t in parsed.get("tables", [])),
            "has_verified_queries": "verified_queries" in parsed,
        }
        
        passed = sum(1 for v in checks.values() if v)
        print(f"✅ YAML validation: {passed}/{len(checks)} checks passed")
        
        for check, status in checks.items():
            print(f"   {'✓' if status else '✗'} {check}")
        
        if parsed.get("tables"):
            table = parsed["tables"][0]
            dims = len(table.get("dimensions", []))
            facts = len(table.get("facts", []))
            print(f"\n   Dimensions: {dims}, Facts: {facts}")
        
        return passed >= 3, parsed
    except Exception as e:
        print(f"❌ Invalid YAML: {str(e)}")
        return False, None


def test_generate_ddr_semantic_model(generator):
    """Generate semantic model for DDR reports."""
    print("\n" + "=" * 60)
    print("STEP 3: Generate Semantic Model for DDR Reports")
    print("=" * 60)
    
    try:
        yaml_content = generator.generate_yaml(
            table_name=f"{DATABASE}.{RAW_SCHEMA}.DAILY_DRILLING_REPORTS",
            model_name="DDR_MODEL",
            business_context="Daily drilling reports with activities, incidents, well information",
            use_llm=True,
        )
        
        print(f"✅ Generated DDR semantic model ({len(yaml_content)} chars)")
        
        parsed = yaml.safe_load(yaml_content)
        if parsed:
            print(f"   Model name: {parsed.get('name', 'N/A')}")
            vqs = parsed.get("verified_queries", [])
            print(f"   Verified queries: {len(vqs)}")
        
        return True
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        return False


def test_save_semantic_model(yaml_content):
    """Save the semantic model to a file."""
    print("\n" + "=" * 60)
    print("STEP 4: Save Semantic Model")
    print("=" * 60)
    
    try:
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "config",
            "semantic_models",
            "drilling_sensor_model.yaml"
        )
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w') as f:
            f.write(yaml_content)
        
        print(f"✅ Saved to: {output_path}")
        return True, output_path
    except Exception as e:
        print(f"❌ Failed to save: {str(e)}")
        return False, None


def run_semantic_model_test():
    """Run the complete semantic model generator test."""
    print("\n" + "=" * 70)
    print("SEMANTIC MODEL GENERATOR E2E TEST")
    print("=" * 70)
    print(f"Database: {DATABASE}")
    print(f"Tables: DRILLING_TIME (12.5M rows), DAILY_DRILLING_REPORTS (1,759 rows)")
    
    results = {}
    
    success, generator, yaml_content = test_generate_drilling_semantic_model()
    results["generate_drilling_model"] = success
    
    if not success or not yaml_content:
        print("\n❌ Cannot continue without semantic model")
        return results
    
    valid, parsed = test_validate_yaml(yaml_content)
    results["validate_yaml"] = valid
    
    results["generate_ddr_model"] = test_generate_ddr_semantic_model(generator)
    
    success, output_path = test_save_semantic_model(yaml_content)
    results["save_model"] = success
    
    print("\n" + "=" * 70)
    print("SEMANTIC MODEL GENERATOR TEST SUMMARY")
    print("=" * 70)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"Results: {passed}/{total} steps passed")
    for step, status in results.items():
        print(f"  {'✅' if status else '❌'} {step}")
    
    if output_path:
        print(f"\n📍 Semantic Model: {output_path}")
    
    return results


if __name__ == "__main__":
    run_semantic_model_test()
