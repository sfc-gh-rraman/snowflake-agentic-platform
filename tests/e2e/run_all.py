"""Unified E2E Test Harness for Agentic Platform.

Runs evaluation datasets through agents and generates pass/fail reports.

Usage:
    python tests/e2e/run_all.py --dataset use_case_parsing
    python tests/e2e/run_all.py --dataset all
    python tests/e2e/run_all.py --dataset plan_generation --output results.json
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.agents.meta_agent import MetaAgent
from src.agents.app_generation.code_generator import AppCodeGenerator


DATASETS_DIR = Path(__file__).parent.parent / "evals" / "datasets"


class EvalResult:
    def __init__(self, test_name: str, passed: bool, details: Dict[str, Any]):
        self.test_name = test_name
        self.passed = passed
        self.details = details
        self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_name": self.test_name,
            "passed": self.passed,
            "details": self.details,
            "timestamp": self.timestamp,
        }


def load_dataset(name: str) -> List[Dict[str, Any]]:
    path = DATASETS_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    with open(path) as f:
        return json.load(f)


def eval_use_case_parsing(test_case: Dict[str, Any]) -> EvalResult:
    meta_agent = MetaAgent()
    
    input_text = test_case["input"]
    expected_domain = test_case["expected_domain"]
    expected_capabilities = set(test_case["expected_capabilities"])
    
    try:
        result = meta_agent.parse_use_case(input_text)
        
        domain_match = result.get("detected_domain", "").lower() == expected_domain.lower()
        detected_caps = set(result.get("capabilities", []))
        caps_overlap = len(expected_capabilities & detected_caps) / len(expected_capabilities) if expected_capabilities else 1.0
        
        passed = domain_match and caps_overlap >= 0.5
        
        return EvalResult(
            test_name=f"use_case_parsing:{expected_domain}",
            passed=passed,
            details={
                "expected_domain": expected_domain,
                "detected_domain": result.get("detected_domain"),
                "domain_match": domain_match,
                "expected_capabilities": list(expected_capabilities),
                "detected_capabilities": list(detected_caps),
                "capability_overlap": caps_overlap,
            }
        )
    except Exception as e:
        return EvalResult(
            test_name=f"use_case_parsing:{expected_domain}",
            passed=False,
            details={"error": str(e)}
        )


def eval_plan_generation(test_case: Dict[str, Any]) -> EvalResult:
    meta_agent = MetaAgent()
    
    input_data = test_case["input"]
    validation = test_case["validation_criteria"]
    
    try:
        result = meta_agent.generate_plan(
            input_data["use_case"],
            input_data.get("data_paths", [])
        )
        
        phases = result.get("phases", [])
        all_agents = []
        for phase in phases:
            agents = phase.get("agents", [])
            for a in agents:
                agent_name = a if isinstance(a, str) else a.get("agent", "")
                all_agents.append(agent_name)
        
        checks = {
            "min_phases": len(phases) >= validation.get("min_phases", 1),
            "required_agents": all(
                agent in all_agents 
                for agent in validation.get("required_agents", [])
            ),
        }
        
        passed = all(checks.values())
        
        return EvalResult(
            test_name=f"plan_generation:{input_data['detected_domain']}",
            passed=passed,
            details={
                "phase_count": len(phases),
                "agents_found": all_agents,
                "checks": checks,
            }
        )
    except Exception as e:
        return EvalResult(
            test_name=f"plan_generation:{input_data['detected_domain']}",
            passed=False,
            details={"error": str(e)}
        )


def eval_code_generation(test_case: Dict[str, Any]) -> EvalResult:
    generator = AppCodeGenerator()
    
    input_data = test_case["input"]
    expected_files = test_case["expected_files"]
    validation = test_case["validation_criteria"]
    
    try:
        result = generator.generate(
            use_case=input_data["use_case"],
            tables=input_data.get("tables", []),
            models=input_data.get("models"),
            search_services=input_data.get("search_services"),
            semantic_models=input_data.get("semantic_models"),
        )
        
        generated_files = list(result.get("files", {}).keys())
        
        checks = {
            "has_frontend": any("frontend" in f for f in generated_files),
            "has_backend": any("backend" in f for f in generated_files),
            "has_deployment": any(f in generated_files for f in ["Dockerfile", "requirements.txt"]),
        }
        
        if validation.get("has_data_fetching"):
            backend_content = "".join(
                v for k, v in result.get("files", {}).items() 
                if "backend" in k
            )
            checks["has_data_fetching"] = "cursor" in backend_content or "execute" in backend_content
        
        passed = all(checks.values())
        
        return EvalResult(
            test_name=f"code_generation:{input_data['app_name']}",
            passed=passed,
            details={
                "generated_files": generated_files,
                "checks": checks,
            }
        )
    except Exception as e:
        return EvalResult(
            test_name=f"code_generation:{input_data['app_name']}",
            passed=False,
            details={"error": str(e)}
        )


def eval_semantic_model(test_case: Dict[str, Any]) -> EvalResult:
    input_data = test_case["input"]
    expected = test_case["expected_semantic_model"]
    
    try:
        columns = input_data["columns"]
        
        inferred_dimensions = []
        inferred_measures = []
        time_dimension = None
        
        for col in columns:
            col_type = col["type"].upper()
            col_name = col["name"]
            
            if "DATE" in col_type or "TIMESTAMP" in col_type:
                if not time_dimension:
                    time_dimension = col_name
                inferred_dimensions.append(col_name)
            elif any(t in col_type for t in ["VARCHAR", "BOOLEAN", "CHAR"]):
                inferred_dimensions.append(col_name)
            elif any(t in col_type for t in ["NUMBER", "INT", "FLOAT", "DECIMAL"]):
                if "_ID" in col_name.upper() or "ID" == col_name.upper():
                    inferred_dimensions.append(col_name)
                else:
                    inferred_measures.append(col_name)
        
        expected_dims = set(expected.get("dimensions", []))
        expected_meas = set(expected.get("measures", []))
        
        dim_overlap = len(expected_dims & set(inferred_dimensions)) / len(expected_dims) if expected_dims else 1.0
        meas_overlap = len(expected_meas & set(inferred_measures)) / len(expected_meas) if expected_meas else 1.0
        time_match = time_dimension == expected.get("time_dimension")
        
        passed = dim_overlap >= 0.7 and meas_overlap >= 0.7 and time_match
        
        return EvalResult(
            test_name=f"semantic_model:{input_data['table']}",
            passed=passed,
            details={
                "inferred_dimensions": inferred_dimensions,
                "inferred_measures": inferred_measures,
                "inferred_time_dimension": time_dimension,
                "dimension_overlap": dim_overlap,
                "measure_overlap": meas_overlap,
                "time_match": time_match,
            }
        )
    except Exception as e:
        return EvalResult(
            test_name=f"semantic_model:{input_data['table']}",
            passed=False,
            details={"error": str(e)}
        )


EVALUATORS = {
    "use_case_parsing": eval_use_case_parsing,
    "plan_generation": eval_plan_generation,
    "code_generation": eval_code_generation,
    "semantic_model": eval_semantic_model,
}


def run_dataset(name: str) -> List[EvalResult]:
    if name not in EVALUATORS:
        raise ValueError(f"Unknown dataset: {name}. Available: {list(EVALUATORS.keys())}")
    
    dataset = load_dataset(name)
    evaluator = EVALUATORS[name]
    
    results = []
    for test_case in dataset:
        result = evaluator(test_case)
        results.append(result)
        status = "✓ PASS" if result.passed else "✗ FAIL"
        print(f"  {status}: {result.test_name}")
    
    return results


def run_all_datasets() -> Dict[str, List[EvalResult]]:
    all_results = {}
    for name in EVALUATORS.keys():
        print(f"\n=== Running {name} ===")
        try:
            all_results[name] = run_dataset(name)
        except FileNotFoundError as e:
            print(f"  Skipped: {e}")
    return all_results


def generate_report(results: Dict[str, List[EvalResult]]) -> Dict[str, Any]:
    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "summary": {},
        "datasets": {},
    }
    
    total_passed = 0
    total_failed = 0
    
    for dataset_name, eval_results in results.items():
        passed = sum(1 for r in eval_results if r.passed)
        failed = sum(1 for r in eval_results if not r.passed)
        total_passed += passed
        total_failed += failed
        
        report["datasets"][dataset_name] = {
            "passed": passed,
            "failed": failed,
            "total": len(eval_results),
            "pass_rate": passed / len(eval_results) if eval_results else 0,
            "results": [r.to_dict() for r in eval_results],
        }
    
    report["summary"] = {
        "total_passed": total_passed,
        "total_failed": total_failed,
        "total_tests": total_passed + total_failed,
        "overall_pass_rate": total_passed / (total_passed + total_failed) if (total_passed + total_failed) > 0 else 0,
    }
    
    return report


def main():
    parser = argparse.ArgumentParser(description="Run E2E evaluation tests")
    parser.add_argument(
        "--dataset",
        choices=list(EVALUATORS.keys()) + ["all"],
        default="all",
        help="Dataset to evaluate",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file for JSON report",
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("Agentic Platform E2E Test Harness")
    print("=" * 60)
    
    if args.dataset == "all":
        results = run_all_datasets()
    else:
        print(f"\n=== Running {args.dataset} ===")
        results = {args.dataset: run_dataset(args.dataset)}
    
    report = generate_report(results)
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total Passed: {report['summary']['total_passed']}")
    print(f"Total Failed: {report['summary']['total_failed']}")
    print(f"Pass Rate: {report['summary']['overall_pass_rate']:.1%}")
    
    for dataset_name, data in report["datasets"].items():
        print(f"\n  {dataset_name}: {data['passed']}/{data['total']} ({data['pass_rate']:.1%})")
    
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\nReport saved to: {output_path}")
    
    sys.exit(0 if report["summary"]["total_failed"] == 0 else 1)


if __name__ == "__main__":
    main()
