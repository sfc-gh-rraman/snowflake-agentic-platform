#!/usr/bin/env python3
"""QA validation suite for Health Sciences orchestrator.

Validates structural integrity of generated orchestrator files against
the skill filesystem and YAML registry.

Usage:
    python scripts/qa_validate_orchestrator.py [--profile incubator|production]
"""

import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKILLS_DIR = ROOT / "skills"
AGENTS_DIR = ROOT / "agents"

ORCH_FILES = {
    "incubator": AGENTS_DIR / "health-sciences-incubator.md",
    "production": AGENTS_DIR / "health-sciences-solutions.md",
}

KNOWN_ROUTER_SUB_SKILLS = {
    "hcls-provider-imaging": {
        "dicom-parser", "dicom-ingestion", "dicom-analytics",
        "imaging-viewer", "imaging-governance", "imaging-ml",
        "data-model-knowledge",
    },
    "hcls-provider-cdata-clinical-docs": {
        "clinical-document-extraction", "clinical-docs-search",
        "clinical-docs-agent", "clinical-docs-viewer",
        "data-model-knowledge",
    },
}


def collect_skill_names(skills_dir: Path) -> dict:
    skill_names = {}
    for skill_md in skills_dir.rglob("SKILL.md"):
        with open(skill_md) as f:
            for line in f:
                if line.startswith("name:"):
                    name = line.strip().replace("name: ", "").strip().strip('"').strip("'")
                    skill_names[name] = {
                        "path": str(skill_md.relative_to(skills_dir)),
                        "folder": skill_md.parent.name,
                        "fullpath": str(skill_md),
                    }
                    break
    return skill_names


def collect_top_level_skills(skill_names: dict) -> dict:
    all_sub_skills = set()
    for subs in KNOWN_ROUTER_SUB_SKILLS.values():
        all_sub_skills.update(subs)
    return {n: v for n, v in skill_names.items() if v["folder"] not in all_sub_skills}


def run_checks(profile: str):
    orch_path = ORCH_FILES[profile]
    if not orch_path.exists():
        print(f"ERROR: {orch_path} not found")
        return 1

    with open(orch_path) as f:
        orch_content = f.read()

    skill_names = collect_skill_names(SKILLS_DIR)
    top_level_skills = collect_top_level_skills(skill_names)
    orch_refs = set(re.findall(r'\$hcls-[a-z0-9-]+', orch_content))

    print("=" * 60)
    print(f"QA VALIDATION REPORT ({profile})")
    print("=" * 60)
    fails = 0

    # CHECK 1: Every $ref in orchestrator has a matching SKILL.md
    print("\n--- CHECK 1: Orchestrator $refs -> SKILL.md name match ---")
    for ref in sorted(orch_refs):
        name = ref[1:]
        if name in skill_names:
            print(f"  PASS: {ref} -> {skill_names[name]['path']}")
        else:
            print(f"  FAIL: {ref} -- no SKILL.md with name: {name}")
            fails += 1

    # CHECK 2: Every top-level SKILL.md referenced in orchestrator
    print("\n--- CHECK 2: Top-level SKILL.md -> orchestrator reference ---")
    for name, info in sorted(top_level_skills.items()):
        if f"${name}" in orch_content:
            print(f"  PASS: {name} -- referenced in orchestrator")
        else:
            print(f"  MISS: {name} ({info['path']}) -- NOT in orchestrator")
            fails += 1

    # CHECK 3: Folder name == SKILL.md name (top-level only)
    print("\n--- CHECK 3: Folder name == SKILL.md name ---")
    for name, info in sorted(top_level_skills.items()):
        if name == info["folder"]:
            print(f"  PASS: {name}")
        else:
            print(f"  FAIL: folder={info['folder']} != name={name} ({info['path']})")
            fails += 1

    # CHECK 4: Router sub-skills exist in filesystem
    print("\n--- CHECK 4: Router sub-skills in filesystem ---")
    for router_name, sub_skills in KNOWN_ROUTER_SUB_SKILLS.items():
        router_dir = SKILLS_DIR / router_name
        for sub in sorted(sub_skills):
            subdir = router_dir / sub
            if subdir.is_dir():
                if sub in orch_content:
                    print(f"  PASS: {router_name}/{sub} -- exists & referenced")
                else:
                    print(f"  WARN: {router_name}/{sub} -- exists but NOT referenced in orchestrator")
            else:
                print(f"  FAIL: {router_name}/{sub} -- directory NOT found")
                fails += 1

    # CHECK 5: Taxonomy tree skill names in orchestrator match filesystem dirs
    print("\n--- CHECK 5: Taxonomy tree entries -> filesystem ---")
    in_tree = False
    tree_skills = []
    for line in orch_content.split("\n"):
        if "```" in line and in_tree:
            break
        if in_tree:
            match = re.search(r'(hcls-[a-z0-9-]+)', line)
            if match:
                tree_skills.append(match.group(1))
        if line.strip() == "```" or "Health Sciences" in line:
            in_tree = True

    for ts in tree_skills:
        if (SKILLS_DIR / ts).is_dir():
            print(f"  PASS: {ts} in tree -> exists in filesystem")
        else:
            print(f"  FAIL: {ts} in tree -> NOT in filesystem")
            fails += 1

    # CHECK 6: Reference consistency
    print("\n--- CHECK 6: Reference consistency (count per skill) ---")
    for ref in sorted(orch_refs):
        count = orch_content.count(ref)
        sections = [
            i for i, line in enumerate(orch_content.split("\n"), 1) if ref in line
        ]
        print(f"  {ref}: {count} occurrences (lines: {sections})")

    # CHECK 7: Standalone skills
    print("\n--- CHECK 7: Standalone skills ---")
    standalone_dir = SKILLS_DIR / "hcls-provider-imaging-dicom-parser"
    if standalone_dir.is_dir():
        if "hcls-provider-imaging-dicom-parser" in skill_names:
            if "$hcls-provider-imaging-dicom-parser" in orch_content:
                print("  PASS: hcls-provider-imaging-dicom-parser exists & referenced")
            else:
                print("  WARN: hcls-provider-imaging-dicom-parser exists but NOT referenced in orchestrator")
        else:
            print("  FAIL: dir exists but no SKILL.md name match")
            fails += 1

    # CHECK 8: Twin orchestrator drift
    print("\n--- CHECK 8: Twin orchestrator drift (incubator vs production) ---")
    other = "production" if profile == "incubator" else "incubator"
    other_path = ORCH_FILES[other]
    if other_path.exists():
        with open(other_path) as f:
            other_content = f.read()

        if "$hcls-" not in other_content:
            print(f"  SKIP: {other} has no skills yet (empty scaffold)")
        else:
            structural_sections = [
                "## Routing Rules",
                "## Skill Routing Tables",
                "## Guardrails",
                "## Getting Started",
                "## Cortex Knowledge Extensions",
            ]

            drift_count = 0
            for section in structural_sections:
                def extract_section(text, header):
                    capturing = False
                    result = []
                    for line in text.split("\n"):
                        if line.strip() == header:
                            capturing = True
                            continue
                        if capturing and line.startswith("## ") and line.strip() != header:
                            break
                        if capturing:
                            result.append(line)
                    return result

                sec_a = [l for l in extract_section(orch_content, section) if l.strip()]
                sec_b = [l for l in extract_section(other_content, section) if l.strip()]

                if sec_a == sec_b:
                    print(f"  PASS: {section} -- identical")
                else:
                    print(f"  FAIL: {section} -- STRUCTURAL DRIFT ({len(sec_a)} vs {len(sec_b)} lines)")
                    drift_count += 1
                    fails += 1

            if drift_count == 0:
                print("  RESULT: No structural drift between orchestrators")
            else:
                print(f"  RESULT: {drift_count} sections have drift -- regenerate from template!")
    else:
        print(f"  SKIP: {other_path.name} not found")

    # CHECK 9: Registry bidirectional — every skill dir has a SKILL.md
    print("\n--- CHECK 9: Skill directories have SKILL.md ---")
    for skill_dir in sorted(SKILLS_DIR.iterdir()):
        if skill_dir.is_dir() and skill_dir.name.startswith("hcls-"):
            if (skill_dir / "SKILL.md").exists():
                print(f"  PASS: {skill_dir.name}/SKILL.md exists")
            else:
                print(f"  FAIL: {skill_dir.name}/SKILL.md MISSING")
                fails += 1

    # CHECK 10: Shared infrastructure exists
    print("\n--- CHECK 10: Shared infrastructure ---")
    shared_checks = [
        ROOT / "shared" / "preflight" / "checker.py",
        ROOT / "shared" / "preflight" / "configs.py",
        ROOT / "shared" / "observability" / "logger.py",
        ROOT / "templates" / "orchestrator.md.j2",
        ROOT / "templates" / "skills_incubator.yaml",
    ]
    for path in shared_checks:
        if path.exists():
            print(f"  PASS: {path.relative_to(ROOT)}")
        else:
            print(f"  FAIL: {path.relative_to(ROOT)} NOT found")
            fails += 1

    # CHECK 11: CKE used_by references point to real skills
    print("\n--- CHECK 11: CKE used_by references ---")
    try:
        import yaml
        registry_path = ROOT / "templates" / f"skills_{profile}.yaml"
        if registry_path.exists():
            with open(registry_path) as f:
                registry = yaml.safe_load(f)
            for name, data in (registry.get("skills") or {}).items():
                if data.get("cke"):
                    for used_by in data.get("used_by", []):
                        clean = used_by.split(" (")[0]
                        if clean in (registry.get("skills") or {}):
                            print(f"  PASS: {name} used_by {clean} -> exists")
                        else:
                            print(f"  FAIL: {name} used_by {clean} -> NOT in registry")
                            fails += 1
        else:
            print(f"  SKIP: {registry_path.name} not found")
    except ImportError:
        print("  SKIP: PyYAML not installed")

    # CHECK 12: Overlap entries reference real skills
    print("\n--- CHECK 12: Overlap entries ---")
    try:
        import yaml
        registry_path = ROOT / "templates" / f"skills_{profile}.yaml"
        if registry_path.exists():
            with open(registry_path) as f:
                registry = yaml.safe_load(f)
            for overlap in registry.get("overlaps", []):
                skill = overlap.get("skill", "")
                if skill in (registry.get("skills") or {}):
                    print(f"  PASS: overlap {skill} -> exists in registry")
                else:
                    print(f"  FAIL: overlap {skill} -> NOT in registry")
                    fails += 1
        else:
            print(f"  SKIP: {registry_path.name} not found")
    except ImportError:
        print("  SKIP: PyYAML not installed")

    # SUMMARY
    print(f"\n{'=' * 60}")
    print(f"TOTAL FAILURES: {fails}")
    print(f"{'=' * 60}")
    return fails


def main():
    parser = argparse.ArgumentParser(description="QA validate orchestrator")
    parser.add_argument(
        "--profile",
        choices=["incubator", "production", "both"],
        default="incubator",
        help="Which profile to validate (default: incubator)",
    )
    args = parser.parse_args()

    profiles = ["incubator", "production"] if args.profile == "both" else [args.profile]
    total_fails = 0
    for p in profiles:
        total_fails += run_checks(p)

    sys.exit(total_fails)


if __name__ == "__main__":
    main()
