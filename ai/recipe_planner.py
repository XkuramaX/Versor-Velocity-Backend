"""
RecipePlanner — Stage 1 of the Planner-Compiler architecture.

Calls the local LLM (Qwen 2.5 14B / DeepSeek-R1 / Llama 3.1 8B) to produce
a simple flat "Logic Recipe": a JSON list of steps, each with a node_type and config.

The LLM never sees React Flow, coordinates, or IDs. It only outputs:
[
  {"node_type": "upload_csv", "config": {}, "label": "employees.csv"},
  {"node_type": "safe_filter", "config": {"filters": [...]}, "label": "Filter salary > 50k"},
  ...
]

This is dramatically simpler for the LLM than generating full React Flow JSON.
"""

import json
import os
import httpx
from ai.node_knowledge import NODE_DOCUMENTS

_ollama_base = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
_ollama_url = f"{_ollama_base}/api/generate"
_ollama_tags = f"{_ollama_base}/api/tags"

# Prefer larger models for better reasoning
PREFERRED_MODELS = [
    "qwen2.5-coder:7b",
    "qwen2.5-coder:3b",
    "qwen2.5-coder:14b",
    "qwen2.5-coder:1.5b",
    "deepseek-r1:8b",
    "llama3.1:8b",
    "llama3.2",
]


def _build_node_catalogue() -> str:
    """Build a compact node catalogue — one line per node, minimal config."""
    lines = []
    for doc in NODE_DOCUMENTS:
        cfg = doc["config_schema"]
        if cfg:
            keys = list(cfg.keys())
            cfg_hint = ", ".join(keys)
        else:
            cfg_hint = "none"
        lines.append(f'  {doc["id"]}: {doc["description"][:80]}  Keys: {cfg_hint}')
    return "\n".join(lines)


NODE_CATALOGUE_TEXT = _build_node_catalogue()

SYSTEM_PROMPT = f"""You are a workflow planner for Versor-Velocity. Convert user requests into a JSON recipe.

Available node_types:
{NODE_CATALOGUE_TEXT}

Output ONLY this JSON:
{{"feasibility":"full"|"partial"|"none","reasoning":"...","recipe":[{{"node_type":"...","config":{{...}},"label":"...","source_file":"..."}}],"gaps":[]}}

Config examples:
- upload_csv: {{}} (first node, set source_file to filename)
- safe_filter: {{"filters":[{{"column":"col","operation":"gt|gte|lt|lte|eq|ne|contains|between|in","value":50000,"column_type":"numeric|string"}}],"logic":"and|or"}}
- math_horizontal: {{"columns":["a","b"],"new_col":"c","op":"sum|average"}}
- math_custom: {{"left_cols":["salary"],"op":"*","right_val":1.1,"new_suffix":"adj"}}
- groupby: {{"group_cols":["dept"],"aggs":{{"salary":["sum","mean","count"]}}}}
- sort: {{"by":["col"],"descending":false}}
- join: {{"on":"id","how":"inner|left|outer"}} (needs join_sources:[0,1] for two upload indices)
- conditional: {{"column":"salary","op":"gt","threshold":50000,"then_val":"High","else_val":"Low","new_col":"band"}}
- select: {{"columns":["a","b"]}}
- rename: {{"mapping":{{"old":"new"}}}}
- string_case: {{"column":"name","mode":"upper|lower|title"}}
- string_clean: {{"column":"name"}}
- fill_missing: {{"column":"col","value":0}}
- cast: {{"column":"age","dtype":"int|float|str|bool"}}

Rules:
1. First step(s) MUST be upload_csv with source_file=filename
2. Use ACTUAL column names from file schemas
3. For Python/pandas code: map df.merge→join, df.groupby→groupby, df[condition]→safe_filter, df['c']=df['a']+df['b']→math_horizontal
4. gaps=[] when feasibility="full". For gaps use: {{"title":"...","description":"...","category":"Machine Learning|Visualization|Data Transformation|Statistics|IO|Other","missing_capability":"..."}}
5. Do NOT report gaps for these (they ARE supported): sum/add columns→math_horizontal, filter rows→safe_filter, label/categorize→conditional, join tables→join, group+aggregate→groupby, create new column from sum→math_horizontal, multiple aggs in groupby→groupby supports ["sum","mean","count"] per column
6. Return ONLY compact JSON (no pretty-printing, no newlines). No markdown fences. No explanation."""


async def _select_model() -> str:
    """Pick the best available model from the preferred list."""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(_ollama_tags)
            available = [m["name"] for m in resp.json().get("models", [])]
    except Exception:
        return os.environ.get("OLLAMA_MODEL", "llama3.2")

    for pref in PREFERRED_MODELS:
        # Exact match
        if pref in available:
            return pref
        # Prefix match (e.g. "qwen2.5-coder:14b" matches "qwen2.5-coder:14b-instruct")
        for avail in available:
            if avail.startswith(pref.split(":")[0]) and pref.split(":")[-1] in avail:
                return avail

    return available[0] if available else os.environ.get("OLLAMA_MODEL", "llama3.2")


async def plan(user_prompt: str, file_schemas: list[dict]) -> dict:
    """
    Call the LLM to produce a Logic Recipe.
    Returns: {"feasibility": ..., "reasoning": ..., "recipe": [...], "gaps": [...]}
    """
    from ai.schema_inferrer import schema_to_prompt_block

    schema_block = schema_to_prompt_block(file_schemas)
    model = await _select_model()
    print(f"[Planner] Using model: {model}")

    user_message = f"""=== FILE SCHEMAS ===
{schema_block}

=== USER REQUEST ===
{user_prompt}

Return the recipe JSON:"""

    full_prompt = f"{SYSTEM_PROMPT}\n\n{user_message}"

    payload = {
        "model": model,
        "prompt": full_prompt,
        "stream": False,
        "options": {
            "temperature": 0.05,
            "num_predict": 8192,
            "num_ctx": 16384,
            "top_p": 0.9,
        },
    }

    async with httpx.AsyncClient(timeout=600) as c:
        resp = await c.post(_ollama_url, json=payload)
        resp.raise_for_status()
        raw = resp.json()["response"]

    return _parse_recipe(raw)


async def plan_refinement(user_prompt: str, current_recipe: list, file_schemas: list[dict]) -> dict:
    """Refine an existing recipe based on a follow-up prompt."""
    from ai.schema_inferrer import schema_to_prompt_block

    schema_block = schema_to_prompt_block(file_schemas)
    model = await _select_model()

    current_json = json.dumps(current_recipe, indent=2)

    user_message = f"""=== FILE SCHEMAS ===
{schema_block}

=== CURRENT RECIPE ===
{current_json}

=== USER MODIFICATION REQUEST ===
{user_prompt}

Modify the recipe above according to the user's request. Keep unchanged steps. Return the COMPLETE updated recipe JSON:"""

    full_prompt = f"{SYSTEM_PROMPT}\n\n{user_message}"

    payload = {
        "model": model,
        "prompt": full_prompt,
        "stream": False,
        "options": {"temperature": 0.05, "num_predict": 8192, "num_ctx": 16384, "top_p": 0.9},
    }

    async with httpx.AsyncClient(timeout=600) as c:
        resp = await c.post(_ollama_url, json=payload)
        resp.raise_for_status()
        raw = resp.json()["response"]

    return _parse_recipe(raw)


def _parse_recipe(raw: str) -> dict:
    """Extract the recipe JSON from LLM output. Handles truncation aggressively."""
    # Strip thinking tags (DeepSeek-R1)
    if "<think>" in raw:
        raw = raw.split("</think>")[-1] if "</think>" in raw else raw.split("<think>")[0]

    # Strip markdown fences
    for fence in ("```json", "```"):
        if fence in raw:
            raw = raw.split(fence, 1)[-1].rsplit("```", 1)[0]

    # Try direct parse
    stripped = raw.strip()
    if stripped.startswith("{"):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

    # Brace-depth extraction
    depth, start = 0, -1
    for i, ch in enumerate(stripped):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start != -1:
                try:
                    return json.loads(stripped[start:i + 1])
                except json.JSONDecodeError:
                    continue

    # Aggressive truncation repair
    if start != -1:
        partial = stripped[start:]
        # Try progressively more aggressive repairs
        for attempt in range(5):
            repair = partial
            # Close any open string
            if repair.count('"') % 2 == 1:
                repair += '"'
            # Close value if we're mid-value (e.g. "value": 6  -> "value": 6)
            # Remove trailing comma or colon
            repair = repair.rstrip().rstrip(',').rstrip(':')
            # If last non-whitespace is a key with no value, add null
            if repair.rstrip().endswith('"'):
                last_quote = repair.rstrip().rfind('"', 0, -1)
                if last_quote >= 0 and repair.rstrip()[last_quote-1:last_quote] in (':', ' '):
                    repair += ': null'
            # Close brackets and braces
            ob = repair.count("{") - repair.count("}")
            oq = repair.count("[") - repair.count("]")
            repair += "]" * max(0, oq)
            repair += "}" * max(0, ob)
            try:
                result = json.loads(repair)
                # Ensure it has the expected structure
                if "recipe" not in result:
                    result["recipe"] = []
                if "feasibility" not in result:
                    result["feasibility"] = "full"
                if "reasoning" not in result:
                    result["reasoning"] = "Parsed from partial LLM output"
                if "gaps" not in result:
                    result["gaps"] = []
                return result
            except json.JSONDecodeError:
                # Remove the last incomplete element and try again
                last_comma = partial.rfind(",")
                if last_comma > 0:
                    partial = partial[:last_comma]
                else:
                    break

    raise ValueError(f"Could not parse recipe from LLM output: {raw[:300]}")
