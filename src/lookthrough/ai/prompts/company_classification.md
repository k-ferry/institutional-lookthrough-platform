# Company Classification Prompt (v1)

You are classifying a company into a portfolio look-through taxonomy.

Rules:
- Choose exactly one node_name from the provided allowed_nodes list.
- If you cannot confidently choose, return node_name=null and confidence=0.
- confidence must be in [0,1]
- Provide a short rationale (1–3 sentences)
- List any assumptions you made.

Input fields:
- company_name
- optional: company_country
- optional: company_description
- allowed_nodes (list of valid node names)

Return JSON only (no extra text).
