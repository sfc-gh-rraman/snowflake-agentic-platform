#!/usr/bin/env python3
"""Generate twin orchestrator .md files from a single Jinja2 template + split YAML registries.

Each profile has its own registry file:
    templates/skills_incubator.yaml   -> agents/health-sciences-incubator.md
    templates/skills_production.yaml  -> agents/health-sciences-solutions.md

Usage:
    python scripts/generate_orchestrators.py [--profile incubator|production|both]
"""

import argparse
import sys
import textwrap
from collections import OrderedDict
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("PyYAML required: pip install pyyaml")

try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
except ImportError:
    sys.exit("Jinja2 required: pip install jinja2")

ROOT = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = ROOT / "templates"
AGENTS_DIR = ROOT / "agents"
REGISTRY_FILES = {
    "incubator": TEMPLATES_DIR / "skills_incubator.yaml",
    "production": TEMPLATES_DIR / "skills_production.yaml",
}
TEMPLATE_FILE = "orchestrator.md.j2"

DOMAIN_ORDER = [
    "Provider > Clinical Research",
    "Provider > Clinical Data Management",
    "Provider > Revenue Cycle",
    "Pharma > Drug Safety",
    "Pharma > Genomics",
    "Pharma > Lab Operations",
    "Cross-Industry > Research Strategy",
    "Cross-Industry > Validation",
]

PROFILE_OUTPUT = {
    "incubator": "health-sciences-incubator.md",
    "production": "health-sciences-solutions.md",
}


class SkillObj:
    def __init__(self, name, data):
        self.name = name
        self.triggers = data.get("triggers", "")
        self.description = data.get("description", "")
        self.domain = data.get("domain", "")
        self.approved = data.get("approved", False)
        self.cke = data.get("cke", False)
        self.standalone = data.get("standalone", False)
        self.data_source = data.get("data_source", "")
        self.used_by = data.get("used_by", [])
        self.invoke_when = data.get("invoke_when", "")
        self.sub_skills = data.get("sub_skills", [])
        self.available = False


def prefix_dollar(name):
    return f"`${name}`"


def load_registry(profile_type):
    path = REGISTRY_FILES[profile_type]
    with open(path) as f:
        return yaml.safe_load(f)


def build_skills(registry, profile_type):
    skills = OrderedDict()
    raw_skills = registry.get("skills") or {}
    for name, data in raw_skills.items():
        skill = SkillObj(name, data)
        skill.available = True
        skills[name] = skill
    return skills


def build_skills_by_domain(skills):
    by_domain = OrderedDict()
    for domain in DOMAIN_ORDER:
        by_domain[domain] = []
    for name, skill in skills.items():
        if skill.available and skill.domain in by_domain:
            by_domain[skill.domain].append((name, skill))
    return by_domain


def filter_patterns(registry, skills):
    available = []
    for pattern in registry.get("patterns", []):
        all_available = True
        for step in pattern["steps"]:
            if "skill" in step:
                skill_name = step["skill"]
                if skill_name in skills and not skills[skill_name].available:
                    all_available = False
                    break
        if all_available:
            available.append(pattern)
    return available


def render(registry, profile_type):
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["prefix_dollar"] = prefix_dollar

    template = env.get_template(TEMPLATE_FILE)

    profile_cfg = registry["profile"]
    skills = build_skills(registry, profile_type)
    skills_by_domain = build_skills_by_domain(skills)
    available_patterns = filter_patterns(registry, skills)

    return template.render(
        profile=profile_cfg,
        skills=skills,
        skills_by_domain=skills_by_domain,
        overlaps=registry.get("overlaps", []),
        available_patterns=available_patterns,
        profile_type=profile_type,
    )


def main():
    parser = argparse.ArgumentParser(description="Generate orchestrator .md files")
    parser.add_argument(
        "--profile",
        choices=["incubator", "production", "both"],
        default="both",
        help="Which profile(s) to generate (default: both)",
    )
    args = parser.parse_args()

    AGENTS_DIR.mkdir(exist_ok=True)

    profiles = ["incubator", "production"] if args.profile == "both" else [args.profile]

    for p in profiles:
        registry = load_registry(p)
        output = render(registry, p)
        out_path = AGENTS_DIR / PROFILE_OUTPUT[p]
        out_path.write_text(output)
        print(f"Generated: {out_path.relative_to(ROOT)}")

    if args.profile == "both" and all((AGENTS_DIR / PROFILE_OUTPUT[p]).exists() for p in ["incubator", "production"]):
        inc = (AGENTS_DIR / PROFILE_OUTPUT["incubator"]).read_text()
        prod = (AGENTS_DIR / PROFILE_OUTPUT["production"]).read_text()
        inc_lines = set(inc.splitlines())
        prod_lines = set(prod.splitlines())

        inc_only = inc_lines - prod_lines
        prod_only = prod_lines - inc_lines

        skill_refs_inc = {l for l in inc_only if "$hcls-" in l}
        skill_refs_prod = {l for l in prod_only if "$hcls-" in l}

        structural_inc = inc_only - skill_refs_inc
        structural_prod = prod_only - skill_refs_prod

        structural_diff = structural_inc | structural_prod
        frontmatter = {l for l in structural_diff if l.startswith("name:") or l.startswith("description:") or l.startswith("# Health")}
        intro_diff = {l for l in structural_diff if "incubator" in l.lower() or "production" in l.lower() or "approved" in l.lower() or "experimental" in l.lower()}

        unexpected = structural_diff - frontmatter - intro_diff
        if unexpected:
            print(f"\nWARNING: {len(unexpected)} unexpected structural differences between orchestrators:")
            for line in sorted(unexpected)[:10]:
                print(f"  {line[:120]}")
        else:
            print("\nDrift check passed: orchestrators differ only in profile metadata and skill availability.")


if __name__ == "__main__":
    main()
