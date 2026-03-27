"""
Agentic RAG Workflow Generator
──────────────────────────────
Multi-step agent loop:
  1. Retrieve relevant nodes from vector store based on user query
  2. Assess feasibility with retrieved context (not the full catalogue)
  3. Generate workflow JSON using only the relevant nodes
  4. Validate the output against the node registry
  5. Retry with corrections if validation fails

This replaces the old prompt-engineering-only approach.
"""

import json
import os
import httpx
from ai.node_knowledge import NODE_DOCUMENTS, get_node_by_id, get_all_node_ids
from ai.vector_store import search_nodes, init_store

_ollama_base = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
_ollama_url = f"{_ollama_base}/api/generate"
_ollama_tags = f"{_ollama_base}/api/tags"
_ollama_model = os.environ.get("OLLAMA_MODEL", "llama3.2")

VALID_NODE_TYPES = set(get_all_node_ids())


async def _llm(prompt: str, max_tokens: int = 8192) -> str:
    """Call Ollama with auto-model-detection."""
    model = _ollama_model
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            tags = await c.get(_ollama_tags)
            available = [m["name"] for m in tags.json().get("models", [])]
            if available and model not in available:
                base = model.split(":")[0]
                model = next((m for m in available if m.startswith(base)), available[0])
    except Exception:
        pass

    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.05, "num_predict": max_tokens, "num_ctx": 16384, "top_p": 0.9},
    }
    async with httpx.AsyncClient(timeout=180) as c:
        resp = await c.post(_ollama_url, json=payload)
        resp.raise_for_status()
        return resp.json()["response"]


def _extract_json(raw: str) -> dict:
    """Robustly extract JSON from LLM output, with truncation repair."""
    cleaned = raw
    for fence in ("```json", "```"):
        if fence in cleaned:
            cleaned = cleaned.split(fence, 1)[-1].rsplit("```", 1)[0]

    stripped = cleaned.strip()
    if stripped.startswith("{"):
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            pass

    depth, start = 0, -1
    for i, ch in enumerate(cleaned):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start != -1:
                try:
                    return json.loads(cleaned[start:i + 1])
                except json.JSONDecodeError:
                    continue

    # Truncation repair
    if start != -1:
        partial = cleaned[start:]
        ob = partial.count("{") - partial.count("}")
        oq = partial.count("[") - partial.count("]")
        repair = partial
        if partial.count('"') % 2 == 1:
            repair += '"'
        repair += "]" * max(0, oq)
        repair += "}" * max(0, ob)
        try:
            return json.loads(repair)
        except json.JSONDecodeError:
            pass

    raise ValueError("No JSON object found in LLM response")


def _validate_workflow(wf: dict) -> tuple[bool, list[str]]:
    """Validate a workflow against the node registry. Returns (is_valid, errors)."""
    errors = []
    nodes = wf.get("nodes", [])
    edges = wf.get("edges", [])

    if not isinstance(nodes, list):
        errors.append("nodes must be a list")
        return False, errors
    if not isinstance(edges, list):
        errors.append("edges must be a list")
        return False, errors

    node_ids = set()
    for n in nodes:
        nid = n.get("id", "")
        if nid in node_ids:
            errors.append(f"Duplicate node id: {nid}")
        node_ids.add(nid)

        node_type = n.get("data", {}).get("nodeType", "")
        if node_type not in VALID_NODE_TYPES:
            errors.append(f"Invalid nodeType '{node_type}' on node {nid}. Valid types: {sorted(VALID_NODE_TYPES)}")

        if n.get("type") != "custom":
            errors.append(f"Node {nid} must have type='custom'")

    for e in edges:
        src = e.get("source", "")
        tgt = e.get("target", "")
        if src not in node_ids:
            errors.append(f"Edge source '{src}' not found in nodes")
        if tgt not in node_ids:
            errors.append(f"Edge target '{tgt}' not found in nodes")

    return len(errors) == 0, errors


# ── Agent steps ───────────────────────────────────────────────────────────────

async def step_retrieve(user_prompt: str, file_schemas: list[dict]) -> list[dict]:
    """Step 1: Retrieve relevant nodes from vector store."""
    # Build a rich query combining user prompt + column names
    schema_text = ""
    for s in file_schemas:
        schema_text += f" columns: {', '.join(s.get('columns', []))}"
    query = f"{user_prompt} {schema_text}".strip()

    results = search_nodes(query, top_k=15)
    # Always include upload_csv since every workflow needs it
    if not any(r["id"] == "upload_csv" for r in results):
        from ai.node_knowledge import build_node_spec
        results.insert(0, {
            "id": "upload_csv",
            "name": "Upload CSV",
            "category": "Data Ingestion",
            "score": 1.0,
            "spec": build_node_spec("upload_csv"),
        })
    return results


async def step_feasibility(user_prompt: str, file_schemas: list[dict], relevant_nodes: list[dict]) -> dict:
    """Step 2: Assess feasibility using only the retrieved nodes."""
    node_specs = "\n\n".join(n["spec"] for n in relevant_nodes)
    all_node_names = ", ".join(sorted(VALID_NODE_TYPES))

    schema_lines = []
    for s in file_schemas:
        schema_lines.append(f"  File '{s['name']}': columns = {s['columns']}")
    schema_text = "\n".join(schema_lines) if schema_lines else "  (no files)"

    prompt = f"""You are a data-engineering analyst. You must assess whether a user request can be fulfilled using ONLY the available nodes below.

=== AVAILABLE NODES (retrieved as most relevant) ===
{node_specs}

=== ALL VALID NODE TYPES IN THE SYSTEM ===
{all_node_names}

=== FILE SCHEMAS ===
{schema_text}

=== USER REQUEST ===
{user_prompt}

=== RULES ===
- feasibility = "full" if EVERY step can be done with the nodes above
- feasibility = "partial" if SOME steps can be done but others cannot
- feasibility = "none" if the CORE requirement cannot be met
- gaps array MUST be empty when feasibility = "full"
- CRITICAL: Do NOT report a gap if ANY combination of existing nodes achieves it:
  * "create column C = A + B" → math_horizontal with op:"sum" → NOT a gap
  * "label rows High/Low" → conditional node → NOT a gap
  * "filter rows" → safe_filter → NOT a gap
  * "join two files" → join node → NOT a gap
- Only report a gap for capabilities that truly do not exist (e.g. neural networks, charts, model export)

Return ONLY valid JSON:
{{
  "feasibility": "full" | "partial" | "none",
  "feasible_steps": ["step description that CAN be done"],
  "gaps": [{{"title": "...", "description": "...", "category": "Machine Learning|Visualization|Data Transformation|Statistics|IO|Other", "missing_capability": "..."}}],
  "reasoning": "one paragraph"
}}"""

    raw = await _llm(prompt)
    return _extract_json(raw)


async def step_generate(feasible_steps: list[str], file_schemas: list[dict], relevant_nodes: list[dict]) -> dict:
    """Step 3: Generate the React Flow workflow JSON."""
    node_specs = "\n\n".join(n["spec"] for n in relevant_nodes)
    steps_text = "\n".join(f"  {i+1}. {s}" for i, s in enumerate(feasible_steps))

    schema_lines = []
    for s in file_schemas:
        schema_lines.append(f"  File '{s['name']}': columns = {s['columns']}")
    schema_text = "\n".join(schema_lines)

    prompt = f"""You are a workflow builder. Generate a React Flow workflow using ONLY these nodes:

=== AVAILABLE NODES ===
{node_specs}

=== FILE SCHEMAS ===
{schema_text}

=== STEPS TO IMPLEMENT ===
{steps_text}

=== OUTPUT FORMAT ===
Return ONLY a JSON object with "nodes" and "edges" arrays.

Node: {{"id": "<type>_<N>", "type": "custom", "position": {{"x": <x>, "y": <y>}}, "data": {{"label": "<label>", "nodeType": "<type>", "config": {{...}}, "status": "idle", "backendNodeId": null}}}}
Edge: {{"id": "e<src>-<tgt>", "source": "<src>", "target": "<tgt>", "type": "smoothstep", "animated": true}}

Rules:
- x starts at 100, increments by 240. Single chain: y=200. Branches: y±160.
- First node(s) MUST be upload_csv (one per input file), label = filename.
- Unique IDs: upload_csv_1, safe_filter_2, math_horizontal_3, etc.
- Config fields MUST match the node specs exactly.
- Column names MUST match the file schemas."""

    raw = await _llm(prompt)
    return _extract_json(raw)


async def step_validate_and_fix(workflow: dict, attempt: int = 0) -> dict:
    """Step 4: Validate and optionally retry."""
    is_valid, errors = _validate_workflow(workflow)
    if is_valid:
        return workflow

    if attempt >= 1:
        # Fix what we can programmatically
        nodes = workflow.get("nodes", [])
        # Fix invalid node types
        for n in nodes:
            nt = n.get("data", {}).get("nodeType", "")
            if nt not in VALID_NODE_TYPES:
                n["data"]["nodeType"] = "select"  # safe fallback
            if n.get("type") != "custom":
                n["type"] = "custom"
        # Deduplicate IDs
        seen = set()
        for n in nodes:
            nid = n.get("id", "node")
            if nid in seen:
                n["id"] = f"{nid}_{len(seen)}"
            seen.add(n["id"])
        return {"nodes": nodes, "edges": workflow.get("edges", [])}

    return workflow


# ── Main orchestrator ─────────────────────────────────────────────────────────

async def generate_workflow_agentic(
    user_prompt: str,
    file_schemas: list[dict],
    output_cols: list[str] = None,
) -> dict:
    """
    Full agentic RAG pipeline.
    Returns: {feasibility, reasoning, workflow, gaps, feasible_steps, retrieved_nodes}
    """
    # Ensure vector store is initialized
    try:
        init_store()
    except Exception as e:
        print(f"[RAG] Vector store init warning: {e}")

    # Step 1: Retrieve relevant nodes
    relevant_nodes = await step_retrieve(user_prompt, file_schemas)
    retrieved_names = [f"{n['name']} ({n['id']})" for n in relevant_nodes]

    # Step 2: Feasibility assessment
    feasibility_result = await step_feasibility(user_prompt, file_schemas, relevant_nodes)
    feasibility = feasibility_result.get("feasibility", "none")
    feasible_steps = feasibility_result.get("feasible_steps", [])
    gaps = feasibility_result.get("gaps", [])
    reasoning = feasibility_result.get("reasoning", "")

    # Step 3: Generate workflow (if feasible)
    workflow = None
    if feasibility in ("full", "partial") and feasible_steps:
        try:
            raw_workflow = await step_generate(feasible_steps, file_schemas, relevant_nodes)
            workflow = await step_validate_and_fix(raw_workflow, attempt=0)

            # Validate again
            is_valid, errors = _validate_workflow(workflow)
            if not is_valid:
                workflow = await step_validate_and_fix(workflow, attempt=1)
        except Exception as e:
            reasoning += f" [Workflow generation failed: {e}]"

    return {
        "feasibility": feasibility,
        "reasoning": reasoning,
        "workflow": workflow,
        "gaps": gaps,
        "feasible_steps": feasible_steps,
        "retrieved_nodes": retrieved_names,
    }


async def refine_workflow_agentic(
    user_prompt: str,
    current_workflow: dict,
    file_schemas: list[dict],
) -> dict:
    """Refine an existing workflow using RAG-retrieved context."""
    try:
        init_store()
    except Exception:
        pass

    relevant_nodes = await step_retrieve(user_prompt, file_schemas)
    node_specs = "\n\n".join(n["spec"] for n in relevant_nodes)

    schema_lines = [f"  File '{s['name']}': columns = {s['columns']}" for s in file_schemas]
    schema_text = "\n".join(schema_lines) if schema_lines else "  (no files)"

    prompt = f"""You are a workflow builder. Modify the existing workflow below according to the user's request.

=== AVAILABLE NODES ===
{node_specs}

=== CURRENT WORKFLOW ===
{json.dumps(current_workflow, indent=2)}

=== FILE SCHEMAS ===
{schema_text}

=== USER REQUEST ===
{user_prompt}

Return the COMPLETE updated workflow as JSON with "nodes" and "edges". Keep unchanged nodes. Only modify what the user asked for.

Node: {{"id": "<type>_<N>", "type": "custom", "position": {{"x": <x>, "y": <y>}}, "data": {{"label": "<label>", "nodeType": "<type>", "config": {{...}}, "status": "idle", "backendNodeId": null}}}}
Edge: {{"id": "e<src>-<tgt>", "source": "<src>", "target": "<tgt>", "type": "smoothstep", "animated": true}}"""

    raw = await _llm(prompt)
    wf = _extract_json(raw)
    validated = await step_validate_and_fix(wf, attempt=0)
    is_valid, _ = _validate_workflow(validated)
    if not is_valid:
        validated = await step_validate_and_fix(validated, attempt=1)
    return {"workflow": validated}
