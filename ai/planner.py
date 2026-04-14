"""
Stage A — The Planner (The Architect)
─────────────────────────────────────
Input:  User prompt + file schemas (columns & types)
Output: A plain-text YAML-like sequence of steps

The LLM outputs simple text like:
  - action: upload_csv
    alias: employees
    source_file: employees.csv
  - action: safe_filter
    input: employees
    config: {column: "salary", op: "gt", value: 60000, column_type: "numeric"}

This is trivially easy for even a 3B model — no JSON structure to maintain.
"""

import os
import re
import yaml
import httpx
from ai.node_knowledge import NODE_DOCUMENTS

_ollama_base = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
_ollama_url = f"{_ollama_base}/api/generate"
_ollama_tags = f"{_ollama_base}/api/tags"

PREFERRED_MODELS = [
    "qwen2.5-coder:7b",
    "qwen2.5-coder:3b",
    "qwen2.5-coder:14b",
    "deepseek-r1:8b",
    "llama3.1:8b",
    "llama3.2",
]

# Build a compact one-liner catalogue
_NODE_LIST = ", ".join(doc["id"] for doc in NODE_DOCUMENTS)
_NODE_DETAILS = "\n".join(
    f'  {doc["id"]}: {doc["description"][:70]}'
    for doc in NODE_DOCUMENTS
)

SYSTEM_PROMPT = f"""You are a data workflow planner. Convert the user's request into a step-by-step recipe.

AVAILABLE ACTIONS:
{_NODE_DETAILS}

OUTPUT FORMAT — plain YAML list, one step per block:
- action: upload_csv
  alias: source
  source_file: employees.csv
- action: safe_filter
  input: source
  config:
    filters:
      - column: salary
        operation: gt
        value: 50000
        column_type: numeric
    logic: and
- action: groupby
  input: safe_filter
  config:
    group_cols: [department]
    aggs:
      salary: [mean, sum]
      name: [count]

RULES:
1. First step(s) MUST be upload_csv with source_file and alias.
2. Each subsequent step has "input" referencing a previous alias or action name.
3. "action" MUST be one of: {_NODE_LIST}
4. Use ACTUAL column names from the file schemas.
5. For joins: two upload_csv steps, then join with input: [alias1, alias2]
6. For pandas code: df[condition]→safe_filter, df['c']=df['a']+df['b']→math_horizontal, df.groupby→groupby, df.merge→join, df.sort_values→sort
7. If a step CANNOT be done with available actions, write: action: UNSUPPORTED, reason: "..."
8. Output ONLY the YAML list. No explanation. No markdown fences."""


async def _select_model() -> str:
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(_ollama_tags)
            available = [m["name"] for m in resp.json().get("models", [])]
    except Exception:
        return os.environ.get("OLLAMA_MODEL", "qwen2.5-coder:7b")

    for pref in PREFERRED_MODELS:
        if pref in available:
            return pref
        for avail in available:
            if avail.startswith(pref.split(":")[0]) and pref.split(":")[-1] in avail:
                return avail
    return available[0] if available else "qwen2.5-coder:7b"


async def plan(user_prompt: str, file_schemas: list[dict]) -> dict:
    """
    Call the LLM to produce a YAML recipe.
    Returns: {"steps": [...], "unsupported": [...], "raw": "..."}
    """
    from ai.schema_inferrer import schema_to_prompt_block
    schema_block = schema_to_prompt_block(file_schemas)
    model = await _select_model()
    print(f"[Planner] Using model: {model}")

    user_msg = f"FILE SCHEMAS:\n{schema_block}\n\nUSER REQUEST:\n{user_prompt}\n\nRecipe:"
    full_prompt = f"{SYSTEM_PROMPT}\n\n{user_msg}"

    payload = {
        "model": model,
        "prompt": full_prompt,
        "stream": False,
        "keep_alive": "24h",
        "options": {"temperature": 0.05, "num_predict": 2048, "num_ctx": 8192, "top_p": 0.9},
    }

    async with httpx.AsyncClient(timeout=600) as c:
        resp = await c.post(_ollama_url, json=payload)
        resp.raise_for_status()
        raw = resp.json()["response"]

    return _parse_yaml_recipe(raw)


async def plan_refinement(user_prompt: str, current_steps: list, file_schemas: list[dict]) -> dict:
    """Refine an existing recipe."""
    from ai.schema_inferrer import schema_to_prompt_block
    schema_block = schema_to_prompt_block(file_schemas)
    model = await _select_model()

    current_yaml = yaml.dump(current_steps, default_flow_style=False)
    user_msg = f"FILE SCHEMAS:\n{schema_block}\n\nCURRENT RECIPE:\n{current_yaml}\n\nMODIFICATION:\n{user_prompt}\n\nUpdated recipe:"
    full_prompt = f"{SYSTEM_PROMPT}\n\n{user_msg}"

    payload = {
        "model": model,
        "prompt": full_prompt,
        "stream": False,
        "keep_alive": "24h",
        "options": {"temperature": 0.05, "num_predict": 2048, "num_ctx": 8192, "top_p": 0.9},
    }

    async with httpx.AsyncClient(timeout=600) as c:
        resp = await c.post(_ollama_url, json=payload)
        resp.raise_for_status()
        raw = resp.json()["response"]

    return _parse_yaml_recipe(raw)


def _parse_yaml_recipe(raw: str) -> dict:
    """Parse the LLM's YAML output into a list of step dicts."""
    # Strip thinking tags (DeepSeek-R1)
    if "<think>" in raw and "</think>" in raw:
        raw = raw.split("</think>", 1)[-1]

    # Strip markdown fences
    for fence in ("```yaml", "```yml", "```"):
        if fence in raw:
            raw = raw.split(fence, 1)[-1].rsplit("```", 1)[0]

    raw = raw.strip()

    # Try YAML parse
    try:
        parsed = yaml.safe_load(raw)
        if isinstance(parsed, list):
            return _categorize_steps(parsed)
    except yaml.YAMLError:
        pass

    # Fallback: line-by-line parsing for malformed YAML
    steps = []
    current = {}
    for line in raw.split("\n"):
        line = line.rstrip()
        if line.startswith("- action:"):
            if current:
                steps.append(current)
            current = {"action": line.split(":", 1)[1].strip()}
        elif line.strip().startswith("alias:") and current:
            current["alias"] = line.split(":", 1)[1].strip()
        elif line.strip().startswith("input:") and current:
            val = line.split(":", 1)[1].strip()
            if val.startswith("["):
                current["input"] = yaml.safe_load(val)
            else:
                current["input"] = val
        elif line.strip().startswith("source_file:") and current:
            current["source_file"] = line.split(":", 1)[1].strip()
        elif line.strip().startswith("reason:") and current:
            current["reason"] = line.split(":", 1)[1].strip().strip('"')
        elif line.strip().startswith("config:") and current:
            # Try to parse the rest as inline YAML
            val = line.split("config:", 1)[1].strip()
            if val and val != "":
                try:
                    current["config"] = yaml.safe_load(val)
                except yaml.YAMLError:
                    current["config"] = {}
            else:
                current["config"] = {}
    if current:
        steps.append(current)

    if not steps:
        raise ValueError(f"Could not parse any steps from LLM output:\n{raw[:500]}")

    return _categorize_steps(steps)


def _categorize_steps(steps: list) -> dict:
    """Separate valid steps from unsupported ones."""
    valid_ids = {doc["id"] for doc in NODE_DOCUMENTS}
    good = []
    unsupported = []
    for s in steps:
        if not isinstance(s, dict):
            continue
        action = s.get("action", "")
        if action == "UNSUPPORTED" or action not in valid_ids:
            unsupported.append(s)
        else:
            good.append(s)
    return {"steps": good, "unsupported": unsupported}
