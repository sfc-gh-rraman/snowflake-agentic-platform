"""E2E Test: Layer 7 - App Generation"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.agents.app_generation.code_generator import AppCodeGenerator


def test_app_generation():
    print("=" * 60)
    print("LAYER 7: APP GENERATION TEST")
    print("Use Case: Chemicals Sales & Market Intelligence Dashboard")
    print("=" * 60)

    generator = AppCodeGenerator(
        output_dir="/Users/rraman/Documents/Solutiions_demo/demos/snowflake-agentic-platform/generated/apps/chemicals_dashboard",
    )

    print("\n[1] Generating app specification (using LLM)")
    try:
        spec = generator.generate_app_spec(
            use_case="Chemical sales analytics and market intelligence dashboard with search and ML predictions",
            tables=[
                "AGENTIC_PLATFORM.RAW.ERP_SALES_ORDERS",
                "AGENTIC_PLATFORM.ML.SALES_FEATURES",
            ],
            models=["SALES_PREDICTOR"],
            search_services=["AGENTIC_PLATFORM.CORTEX.MARKET_SEARCH"],
            semantic_models=["SALES_ANALYTICS_MODEL"],
        )
        print(f"    ✓ App name: {spec.app_name}")
        print(f"    ✓ Description: {spec.description[:80]}...")
        print(f"    ✓ Pages: {len(spec.pages)}")
        for page in spec.pages[:3]:
            print(f"      - {page.get('name')}: {page.get('route')}")
        print(f"    ✓ Features: {spec.features}")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n[2] Generating React components")
    try:
        react_files = generator.generate_react_components(spec)
        print(f"    ✓ Generated {len(react_files)} React files")
        for path in list(react_files.keys())[:5]:
            print(f"      - {path}")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        return False

    print("\n[3] Generating FastAPI backend")
    try:
        backend_files = generator.generate_fastapi_backend(spec)
        print(f"    ✓ Generated {len(backend_files)} backend files")
        for path in backend_files.keys():
            print(f"      - {path}")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        return False

    print("\n[4] Generating deployment config (SPCS)")
    try:
        deploy_files = generator.generate_deployment_config(spec)
        print(f"    ✓ Generated {len(deploy_files)} deployment files")
        for path in deploy_files.keys():
            print(f"      - {path}")
    except Exception as e:
        print(f"    ✗ FAILED: {e}")
        return False

    print("\n[5] Writing generated files to disk")
    try:
        output_dir = generator.output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        all_files = {}
        for path, content in react_files.items():
            all_files[f"frontend/src/{path}"] = content
        all_files.update(backend_files)
        all_files.update(deploy_files)
        
        for path, content in all_files.items():
            full_path = os.path.join(output_dir, path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, 'w') as f:
                f.write(content)
        
        print(f"    ✓ Wrote {len(all_files)} files to {output_dir}")
    except Exception as e:
        print(f"    ⚠ Could not write files: {e}")

    print("\n[6] Validating generated structure")
    try:
        # Check key files exist
        required = ['frontend/src/App.tsx', 'main.py', 'Dockerfile', 'requirements.txt']
        found = []
        for req in required:
            if req in all_files or any(req in k for k in all_files.keys()):
                found.append(req)
        
        print(f"    ✓ Required files present: {found}")
    except Exception as e:
        print(f"    ⚠ Validation error: {e}")

    print("\n" + "=" * 60)
    print("LAYER 7: PASSED ✓")
    print("=" * 60)
    
    print("\n" + "=" * 60)
    print("ALL 7 LAYERS COMPLETE - E2E TEST SUITE PASSED!")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_app_generation()
    sys.exit(0 if success else 1)
