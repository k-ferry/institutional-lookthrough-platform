"""Entity Resolution Module for Institutional Look-Through Platform.

Matches raw company names in fact_reported_holding to canonical company_id
using dim_company and dim_entity_alias.

Resolution strategies (applied in order):
1. Exact match (case-insensitive) - confidence 1.0
2. Alias match (case-insensitive) - confidence 0.95
3. Normalized match (stripped suffixes/punctuation) - confidence 0.90
4. Token overlap match (Jaccard similarity >= 0.70) - confidence 0.80
5. First entity match (first company in multi-company names) - confidence 0.75
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


# Common company suffixes to strip during normalization
COMPANY_SUFFIXES = {
    "inc", "llc", "lp", "l.p.", "corp", "corporation", "ltd", "limited",
    "co", "holdings", "group", "holdco", "parent", "topco", "bidco",
    "buyer", "acquiror", "investor", "acquisition", "purchaser"
}

# Connector words to strip
CONNECTOR_WORDS = {"and", "the", "of", "dba", "fka", "aka"}


def _repo_root() -> Path:
    # src/lookthrough/inference/entity_resolution.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return pd.read_csv(path)


def _is_null(value) -> bool:
    """Check if value is null, NaN, or string 'nan'."""
    if value is None:
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.lower() in ("nan", "none", ""):
        return True
    return False


def _normalize_name(name: str) -> str:
    """
    Normalize a company name by:
    - Converting to lowercase
    - Removing content in parentheses (e.g., "(dba Aptean)")
    - Removing punctuation (commas, periods, parentheses)
    - Removing common suffixes (Inc, LLC, Corp, etc.)
    - Removing connector words (and, the, of, dba, fka, aka)
    - Collapsing whitespace
    """
    if not name:
        return ""

    # Lowercase
    text = name.lower().strip()

    # Remove content in parentheses like "(dba Aptean)" or "(fka The Step2 Company)"
    text = re.sub(r'\([^)]*\)', '', text)

    # Remove punctuation: commas, periods, parentheses
    text = re.sub(r'[,.\(\)]', ' ', text)

    # Split into words
    words = text.split()

    # Filter out suffixes and connector words
    filtered_words = []
    for word in words:
        word_clean = word.strip()
        if word_clean and word_clean not in COMPANY_SUFFIXES and word_clean not in CONNECTOR_WORDS:
            filtered_words.append(word_clean)

    # Rejoin and collapse whitespace
    return ' '.join(filtered_words)


def _tokenize(name: str) -> set[str]:
    """
    Tokenize a normalized name into a set of words.
    """
    normalized = _normalize_name(name)
    if not normalized:
        return set()
    return set(normalized.split())


def _jaccard_similarity(set1: set[str], set2: set[str]) -> float:
    """
    Calculate Jaccard similarity between two sets.
    """
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def _extract_first_entity(name: str) -> str:
    """
    Extract the first entity from a multi-entity company name.

    Examples:
    - "CompanyA LLC and CompanyB Holdings LP" -> "CompanyA LLC"
    - "Mustang Prospects Holdco, LLC, Mustang Prospects Purchaser, LLC and ..." -> "Mustang Prospects Holdco, LLC"
    """
    if not name:
        return ""

    # First try splitting by " and " (with spaces to avoid splitting "Grand" etc.)
    # But need to be careful - some company names have ", LLC and " patterns
    # So let's split by patterns like ", LLC and", ", Inc. and", etc.

    # Pattern: split on ", LLC" or ", Inc" etc. followed by more content
    # Actually, let's find the first occurrence of " and " that's NOT part of a company name
    # This is tricky. Let's use a simpler heuristic:
    # Split on " and " but only if what comes before looks like a complete company name

    text = name.strip()

    # Look for patterns like "CompanyA, LLC, CompanyB, Inc." - split on the comma after LLC/Inc
    # Pattern: after a suffix word followed by comma
    suffix_pattern = r'(?:inc|llc|lp|l\.p\.|corp|corporation|ltd|limited)\s*,'
    match = re.search(suffix_pattern, text, re.IGNORECASE)
    if match:
        # Take everything up to and including the suffix (not the comma)
        first_part = text[:match.end()-1].strip()  # -1 to exclude the comma
        return first_part

    # Try splitting on " and " - take the first part
    and_pos = text.lower().find(' and ')
    if and_pos > 0:
        first_part = text[:and_pos].strip()
        # Make sure we got something meaningful (at least 3 chars)
        if len(first_part) >= 3:
            return first_part

    # No split point found, return original
    return text


def resolve_entities(verbose: bool = False) -> pd.DataFrame:
    """
    Resolve raw company names to canonical company_id.

    Resolution strategy (applied in order):
    1. Exact match (case-insensitive) against dim_company.company_name - confidence 1.0
    2. Exact match (case-insensitive) against dim_entity_alias.alias_text - confidence 0.95
    3. Normalized match (stripped suffixes/punctuation) - confidence 0.90
    4. Token overlap match (Jaccard similarity >= 0.70) - confidence 0.80
    5. First entity match (first company in multi-company names) - confidence 0.75

    Args:
        verbose: If True, print examples of matches found by each method

    Returns:
        Updated holdings DataFrame
    """
    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    # Load required data
    holdings = _read_csv(silver / "fact_reported_holding.csv")
    companies = _read_csv(silver / "dim_company.csv")
    aliases = _read_csv(silver / "dim_entity_alias.csv")

    # Build lookup dictionaries (case-insensitive)
    # Direct company name -> company_id
    company_name_to_id: dict[str, str] = {}
    company_id_to_name: dict[str, str] = {}  # Reverse lookup for logging
    for _, row in companies.iterrows():
        name_lower = str(row["company_name"]).lower().strip()
        company_id = str(row["company_id"])
        company_name_to_id[name_lower] = company_id
        company_id_to_name[company_id] = str(row["company_name"])

    # Alias text -> company_id (only for entity_type='company')
    alias_to_company_id: dict[str, str] = {}
    for _, row in aliases.iterrows():
        if str(row.get("entity_type", "")).lower() == "company":
            alias_lower = str(row["alias_text"]).lower().strip()
            alias_to_company_id[alias_lower] = str(row["entity_id"])

    # Build normalized name lookup
    normalized_to_company_id: dict[str, str] = {}
    for name_lower, company_id in company_name_to_id.items():
        normalized = _normalize_name(name_lower)
        if normalized and normalized not in normalized_to_company_id:
            normalized_to_company_id[normalized] = company_id

    # Build tokenized lookup for Jaccard similarity
    company_tokens: dict[str, tuple[set[str], str]] = {}  # normalized -> (tokens, company_id)
    for name_lower, company_id in company_name_to_id.items():
        tokens = _tokenize(name_lower)
        if tokens:
            normalized = _normalize_name(name_lower)
            if normalized not in company_tokens:
                company_tokens[normalized] = (tokens, company_id)

    # Track resolution statistics
    resolved_direct = 0
    resolved_alias = 0
    resolved_normalized = 0
    resolved_token_overlap = 0
    resolved_first_entity = 0
    unresolved = 0
    already_resolved = 0

    # Track examples for each method
    examples_normalized: list[tuple[str, str]] = []
    examples_token_overlap: list[tuple[str, str, float]] = []
    examples_first_entity: list[tuple[str, str, str]] = []

    # Resolution log entries
    resolution_log: list[dict] = []

    # Process each holding
    for idx, row in holdings.iterrows():
        holding_id = str(row["reported_holding_id"])
        raw_name = str(row.get("raw_company_name", ""))
        raw_name_lower = raw_name.lower().strip()
        current_company_id = row.get("company_id")

        # Skip if already has a valid company_id
        if not _is_null(current_company_id):
            already_resolved += 1
            continue

        matched_company_id: Optional[str] = None
        match_method = "unresolved"
        match_confidence = 0.0
        matched_to_name: Optional[str] = None

        # 1. Try direct match against company_name
        if raw_name_lower in company_name_to_id:
            matched_company_id = company_name_to_id[raw_name_lower]
            match_method = "direct"
            match_confidence = 1.0
            resolved_direct += 1

        # 2. Try alias match
        elif raw_name_lower in alias_to_company_id:
            matched_company_id = alias_to_company_id[raw_name_lower]
            match_method = "alias"
            match_confidence = 0.95
            resolved_alias += 1

        # 3. Try normalized match
        else:
            raw_normalized = _normalize_name(raw_name)
            if raw_normalized and raw_normalized in normalized_to_company_id:
                matched_company_id = normalized_to_company_id[raw_normalized]
                match_method = "normalized"
                match_confidence = 0.90
                matched_to_name = company_id_to_name.get(matched_company_id, "")
                resolved_normalized += 1
                if len(examples_normalized) < 5:
                    examples_normalized.append((raw_name, matched_to_name))

            # 4. Try token overlap match (Jaccard similarity)
            if matched_company_id is None:
                raw_tokens = _tokenize(raw_name)
                if raw_tokens and len(raw_tokens) >= 2:  # Require at least 2 tokens
                    best_match: Optional[tuple[str, float]] = None
                    best_similarity = 0.0

                    for normalized_name, (tokens, company_id) in company_tokens.items():
                        similarity = _jaccard_similarity(raw_tokens, tokens)
                        if similarity >= 0.70 and similarity > best_similarity:
                            best_similarity = similarity
                            best_match = (company_id, similarity)

                    if best_match:
                        matched_company_id = best_match[0]
                        match_method = "token_overlap"
                        match_confidence = 0.80
                        matched_to_name = company_id_to_name.get(matched_company_id, "")
                        resolved_token_overlap += 1
                        if len(examples_token_overlap) < 5:
                            examples_token_overlap.append((raw_name, matched_to_name, best_similarity))

            # 5. Try first entity match
            if matched_company_id is None:
                first_entity = _extract_first_entity(raw_name)
                if first_entity and first_entity.lower() != raw_name_lower:
                    first_normalized = _normalize_name(first_entity)
                    if first_normalized and first_normalized in normalized_to_company_id:
                        matched_company_id = normalized_to_company_id[first_normalized]
                        match_method = "first_entity"
                        match_confidence = 0.75
                        matched_to_name = company_id_to_name.get(matched_company_id, "")
                        resolved_first_entity += 1
                        if len(examples_first_entity) < 5:
                            examples_first_entity.append((raw_name, first_entity, matched_to_name))

        if matched_company_id is None:
            unresolved += 1

        # Update holdings DataFrame if matched
        if matched_company_id is not None:
            holdings.at[idx, "company_id"] = matched_company_id

        # Log the resolution attempt
        resolution_log.append({
            "reported_holding_id": holding_id,
            "raw_company_name": raw_name,
            "matched_company_id": matched_company_id,
            "match_method": match_method,
            "match_confidence": match_confidence,
        })

    # Write updated holdings back to silver
    holdings_path = silver / "fact_reported_holding.csv"
    holdings.to_csv(holdings_path, index=False)

    # Write resolution log to gold
    resolution_log_df = pd.DataFrame(resolution_log)
    log_path = gold / "entity_resolution_log.csv"
    resolution_log_df.to_csv(log_path, index=False)

    # Calculate totals
    total_processed = (resolved_direct + resolved_alias + resolved_normalized +
                       resolved_token_overlap + resolved_first_entity + unresolved)
    total_resolved = (resolved_direct + resolved_alias + resolved_normalized +
                      resolved_token_overlap + resolved_first_entity)
    total_new_methods = resolved_normalized + resolved_token_overlap + resolved_first_entity

    # Print summary statistics
    print("=" * 60)
    print("Entity Resolution Summary")
    print("=" * 60)
    print(f"Already resolved (skipped):       {already_resolved:,}")
    print(f"Processed (null company_id):      {total_processed:,}")
    print("-" * 60)
    print("Resolution by method:")
    print(f"  1. Direct match (1.00):         {resolved_direct:,}")
    print(f"  2. Alias match (0.95):          {resolved_alias:,}")
    print(f"  3. Normalized match (0.90):     {resolved_normalized:,}")
    print(f"  4. Token overlap match (0.80):  {resolved_token_overlap:,}")
    print(f"  5. First entity match (0.75):   {resolved_first_entity:,}")
    print(f"  Unresolved:                     {unresolved:,}")
    print("-" * 60)
    print(f"NEW methods resolved:             {total_new_methods:,}")
    print(f"Total resolved:                   {total_resolved:,}")
    if total_processed > 0:
        resolution_rate = total_resolved / total_processed * 100
        print(f"Resolution rate:                  {resolution_rate:.1f}%")
    print()

    # Print examples if verbose or if there are new method matches
    if verbose or total_new_methods > 0:
        if examples_normalized:
            print("Examples - Normalized Match:")
            for raw, matched in examples_normalized[:3]:
                print(f"  '{raw}' -> '{matched}'")
            print()

        if examples_token_overlap:
            print("Examples - Token Overlap Match:")
            for raw, matched, sim in examples_token_overlap[:3]:
                print(f"  '{raw}' -> '{matched}' (similarity: {sim:.2f})")
            print()

        if examples_first_entity:
            print("Examples - First Entity Match:")
            for raw, first, matched in examples_first_entity[:3]:
                print(f"  '{raw}'")
                print(f"    -> first entity: '{first}'")
                print(f"    -> matched: '{matched}'")
            print()

    print(f"Wrote updated holdings: {holdings_path}")
    print(f"Wrote resolution log:   {log_path}")

    return holdings


def analyze_potential_matches() -> None:
    """
    Analyze potential matches that the new resolution methods would find.

    This function doesn't modify any data - it only reports what additional
    matches would be found by the normalized, token overlap, and first entity
    matching methods.
    """
    root = _repo_root()
    silver = root / "data" / "silver"

    # Load required data
    holdings = _read_csv(silver / "fact_reported_holding.csv")
    companies = _read_csv(silver / "dim_company.csv")
    aliases = _read_csv(silver / "dim_entity_alias.csv")

    # Build lookup dictionaries
    company_name_to_id: dict[str, str] = {}
    company_id_to_name: dict[str, str] = {}
    for _, row in companies.iterrows():
        name_lower = str(row["company_name"]).lower().strip()
        company_id = str(row["company_id"])
        company_name_to_id[name_lower] = company_id
        company_id_to_name[company_id] = str(row["company_name"])

    alias_to_company_id: dict[str, str] = {}
    for _, row in aliases.iterrows():
        if str(row.get("entity_type", "")).lower() == "company":
            alias_lower = str(row["alias_text"]).lower().strip()
            alias_to_company_id[alias_lower] = str(row["entity_id"])

    # Build normalized and token lookups
    normalized_to_company_id: dict[str, str] = {}
    company_tokens: dict[str, tuple[set[str], str]] = {}
    for name_lower, company_id in company_name_to_id.items():
        normalized = _normalize_name(name_lower)
        if normalized and normalized not in normalized_to_company_id:
            normalized_to_company_id[normalized] = company_id
        tokens = _tokenize(name_lower)
        if tokens and normalized not in company_tokens:
            company_tokens[normalized] = (tokens, company_id)

    # Get unique raw company names that have null company_id
    unresolved_names: set[str] = set()
    for _, row in holdings.iterrows():
        if _is_null(row.get("company_id")):
            raw_name = str(row.get("raw_company_name", ""))
            if raw_name and raw_name.lower() not in ("nan", "none", ""):
                unresolved_names.add(raw_name)

    # Also get names that ARE resolved but could potentially match other companies
    # (for analysis of what the methods would do)
    all_names: set[str] = set()
    for _, row in holdings.iterrows():
        raw_name = str(row.get("raw_company_name", ""))
        if raw_name and raw_name.lower() not in ("nan", "none", ""):
            all_names.add(raw_name)

    print("=" * 70)
    print("Entity Resolution Analysis - Potential Matches")
    print("=" * 70)
    print(f"Total unique company names in holdings: {len(all_names):,}")
    print(f"Currently unresolved (null company_id): {len(unresolved_names):,}")
    print(f"Total companies in dim_company: {len(company_name_to_id):,}")
    print()

    # Analyze each method
    normalized_matches: list[tuple[str, str]] = []
    token_matches: list[tuple[str, str, float]] = []
    first_entity_matches: list[tuple[str, str, str]] = []

    for raw_name in all_names:
        raw_name_lower = raw_name.lower().strip()

        # Skip if exact match exists
        if raw_name_lower in company_name_to_id:
            continue
        if raw_name_lower in alias_to_company_id:
            continue

        # Try normalized match
        raw_normalized = _normalize_name(raw_name)
        if raw_normalized and raw_normalized in normalized_to_company_id:
            matched_id = normalized_to_company_id[raw_normalized]
            matched_name = company_id_to_name.get(matched_id, "")
            if matched_name.lower() != raw_name_lower:  # Not self-match
                normalized_matches.append((raw_name, matched_name))
            continue

        # Try token overlap
        raw_tokens = _tokenize(raw_name)
        if raw_tokens and len(raw_tokens) >= 2:
            best_match = None
            best_similarity = 0.0
            for normalized_name, (tokens, company_id) in company_tokens.items():
                similarity = _jaccard_similarity(raw_tokens, tokens)
                if similarity >= 0.70 and similarity > best_similarity:
                    best_similarity = similarity
                    best_match = (company_id, similarity)
            if best_match:
                matched_name = company_id_to_name.get(best_match[0], "")
                if matched_name.lower() != raw_name_lower:
                    token_matches.append((raw_name, matched_name, best_match[1]))
                continue

        # Try first entity match
        first_entity = _extract_first_entity(raw_name)
        if first_entity and first_entity.lower() != raw_name_lower:
            first_normalized = _normalize_name(first_entity)
            if first_normalized and first_normalized in normalized_to_company_id:
                matched_id = normalized_to_company_id[first_normalized]
                matched_name = company_id_to_name.get(matched_id, "")
                first_entity_matches.append((raw_name, first_entity, matched_name))

    # Print results
    print(f"NORMALIZED MATCH would find: {len(normalized_matches)} additional matches")
    if normalized_matches:
        print("  Examples:")
        for raw, matched in normalized_matches[:5]:
            print(f"    '{raw}'")
            print(f"      -> '{matched}'")
        print()

    print(f"TOKEN OVERLAP MATCH would find: {len(token_matches)} additional matches")
    if token_matches:
        print("  Examples:")
        for raw, matched, sim in token_matches[:5]:
            print(f"    '{raw}'")
            print(f"      -> '{matched}' (Jaccard: {sim:.2f})")
        print()

    print(f"FIRST ENTITY MATCH would find: {len(first_entity_matches)} additional matches")
    if first_entity_matches:
        print("  Examples:")
        for raw, first, matched in first_entity_matches[:5]:
            print(f"    '{raw}'")
            print(f"      -> first entity: '{first}'")
            print(f"      -> matched: '{matched}'")
        print()

    total_new = len(normalized_matches) + len(token_matches) + len(first_entity_matches)
    print("=" * 70)
    print(f"TOTAL ADDITIONAL MATCHES: {total_new}")
    print("=" * 70)


def _pick_canonical_name(names: list[str]) -> tuple[str, int]:
    """
    Pick the shortest, cleanest company name as the canonical version.

    Returns (canonical_name, index_of_canonical).
    """
    if not names:
        return "", -1

    def name_score(name: str) -> tuple[int, int, int]:
        """Lower score = better canonical candidate."""
        # Prefer shorter names
        length = len(name)
        # Prefer names without " and " (single entities)
        has_and = 1 if " and " in name.lower() else 0
        # Prefer names without parentheses (no dba/fka notes)
        has_parens = 1 if "(" in name else 0
        return (has_and, has_parens, length)

    scored = [(name_score(n), i, n) for i, n in enumerate(names)]
    scored.sort()
    return scored[0][2], scored[0][1]


def consolidate_company_duplicates_safe() -> dict:
    """
    Safely consolidate duplicate companies using conservative matching rules.

    Safety rules:
    1. Normalized duplicates: Only if all variants share the same first meaningful word
       AND have more than one meaningful word (skip single-word matches like "summit")
    2. Token overlap: Only if Jaccard similarity >= 0.90 (not 0.70)
    3. First entity: Only if the first entity name has 2+ words

    For each consolidation:
    - Pick shortest clean name as canonical
    - Update holdings to point to canonical company_id
    - Add duplicate names to dim_entity_alias.csv
    - Log to entity_resolution_log with action "company_consolidation"
    - Do NOT delete duplicate dim_company rows (preserve audit trail)

    Returns:
        Dict with statistics about consolidation.
    """
    import datetime

    root = _repo_root()
    silver = root / "data" / "silver"
    gold = root / "data" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    companies = _read_csv(silver / "dim_company.csv")
    holdings = _read_csv(silver / "fact_reported_holding.csv")

    # Load existing aliases
    alias_path = silver / "dim_entity_alias.csv"
    if alias_path.exists():
        aliases = _read_csv(alias_path)
    else:
        aliases = pd.DataFrame(columns=["alias_id", "entity_type", "entity_id", "alias_text"])

    # Build lookups
    company_id_to_name: dict[str, str] = {}
    for _, row in companies.iterrows():
        company_id = str(row["company_id"])
        company_id_to_name[company_id] = str(row["company_name"])

    # Track consolidation groups with reasons
    # Each entry: (canonical_id, [duplicate_ids], method, reason)
    consolidation_groups: list[tuple[str, list[str], str, str]] = []
    skipped: list[tuple[str, str, str]] = []  # (normalized, method, reason)

    # Track which company_ids are already in a group
    already_grouped: set[str] = set()

    # ============================================================
    # 1. NORMALIZED DUPLICATES (with safety checks)
    # ============================================================
    normalized_groups: dict[str, list[tuple[str, str]]] = {}  # normalized -> [(company_id, original_name)]
    for company_id, company_name in company_id_to_name.items():
        normalized = _normalize_name(company_name)
        if normalized:
            if normalized not in normalized_groups:
                normalized_groups[normalized] = []
            normalized_groups[normalized].append((company_id, company_name))

    for normalized, members in normalized_groups.items():
        if len(members) <= 1:
            continue

        # Safety check: must have more than one meaningful word
        words = normalized.split()
        if len(words) < 2:
            skipped.append((normalized, "normalized",
                           f"single-word match '{normalized}' too generic"))
            continue

        # Safety check: all variants must share the same first meaningful word
        first_words = set()
        for _, original_name in members:
            orig_normalized = _normalize_name(original_name)
            if orig_normalized:
                orig_words = orig_normalized.split()
                if orig_words:
                    first_words.add(orig_words[0])

        if len(first_words) > 1:
            skipped.append((normalized, "normalized",
                           f"variants have different first words: {first_words}"))
            continue

        # Safe to consolidate - pick canonical
        names = [name for _, name in members]
        canonical_name, canonical_idx = _pick_canonical_name(names)
        canonical_id = members[canonical_idx][0]
        duplicate_ids = [cid for cid, _ in members if cid != canonical_id]

        consolidation_groups.append((canonical_id, duplicate_ids, "normalized",
                                     f"normalized='{normalized}'"))
        already_grouped.add(canonical_id)
        already_grouped.update(duplicate_ids)

    # ============================================================
    # 2. TOKEN OVERLAP (Jaccard >= 0.90 only)
    # ============================================================
    token_data: list[tuple[str, str, set[str]]] = []  # (company_id, name, tokens)
    for company_id, company_name in company_id_to_name.items():
        if company_id in already_grouped:
            continue
        normalized = _normalize_name(company_name)
        tokens = set(normalized.split()) if normalized else set()
        if tokens and len(tokens) >= 2:
            token_data.append((company_id, company_name, tokens))

    processed_token: set[str] = set()
    for i, (cid1, name1, tokens1) in enumerate(token_data):
        if cid1 in processed_token:
            continue

        similar_group = [(cid1, name1)]
        for cid2, name2, tokens2 in token_data[i+1:]:
            if cid2 in processed_token:
                continue

            similarity = _jaccard_similarity(tokens1, tokens2)
            if similarity >= 0.90:  # Stricter threshold
                similar_group.append((cid2, name2))
                processed_token.add(cid2)
            elif similarity >= 0.70:
                # Would have matched with old threshold - skip and log
                skipped.append((f"{name1} <-> {name2}", "token_overlap",
                               f"Jaccard {similarity:.2f} < 0.90 threshold"))

        if len(similar_group) > 1:
            names = [name for _, name in similar_group]
            canonical_name, canonical_idx = _pick_canonical_name(names)
            canonical_id = similar_group[canonical_idx][0]
            duplicate_ids = [cid for cid, _ in similar_group if cid != canonical_id]

            consolidation_groups.append((canonical_id, duplicate_ids, "token_overlap",
                                        f"Jaccard >= 0.90"))
            already_grouped.add(canonical_id)
            already_grouped.update(duplicate_ids)

        processed_token.add(cid1)

    # ============================================================
    # 3. FIRST ENTITY MATCHES (first entity must have 2+ words)
    # ============================================================
    for company_id, company_name in company_id_to_name.items():
        if company_id in already_grouped:
            continue

        first_entity = _extract_first_entity(company_name)
        if first_entity.lower() == company_name.lower():
            continue  # Not a multi-entity name

        # Safety check: first entity must have 2+ words
        first_normalized = _normalize_name(first_entity)
        if not first_normalized:
            continue
        first_words = first_normalized.split()
        if len(first_words) < 2:
            skipped.append((company_name, "first_entity",
                           f"first entity '{first_entity}' has <2 words"))
            continue

        # Find matching company
        for other_id, other_name in company_id_to_name.items():
            if other_id == company_id or other_id in already_grouped:
                continue
            other_normalized = _normalize_name(other_name)
            if other_normalized == first_normalized:
                # The multi-entity name should point to the simpler name
                names = [company_name, other_name]
                canonical_name, canonical_idx = _pick_canonical_name(names)
                if canonical_idx == 0:
                    canonical_id, duplicate_ids = company_id, [other_id]
                else:
                    canonical_id, duplicate_ids = other_id, [company_id]

                consolidation_groups.append((canonical_id, duplicate_ids, "first_entity",
                                            f"first entity='{first_entity}'"))
                already_grouped.add(canonical_id)
                already_grouped.update(duplicate_ids)
                break

    # ============================================================
    # BUILD CONSOLIDATION MAP AND APPLY CHANGES
    # ============================================================
    consolidation_map: dict[str, str] = {}  # old_id -> canonical_id
    for canonical_id, duplicate_ids, method, reason in consolidation_groups:
        for dup_id in duplicate_ids:
            consolidation_map[dup_id] = canonical_id

    # Count and update holdings
    holdings_updated = 0
    for idx, row in holdings.iterrows():
        current_company_id = row.get("company_id")
        if not _is_null(current_company_id):
            cid = str(current_company_id)
            if cid in consolidation_map:
                holdings.at[idx, "company_id"] = consolidation_map[cid]
                holdings_updated += 1

    # Add duplicate names to aliases
    new_aliases = []
    existing_aliases = set(aliases["alias_text"].str.lower() if len(aliases) > 0 else [])
    max_alias_id = 0
    if len(aliases) > 0 and "alias_id" in aliases.columns:
        # Extract numeric part from alias IDs like "alias_001"
        for aid in aliases["alias_id"]:
            if str(aid).startswith("alias_"):
                try:
                    num = int(str(aid).split("_")[1])
                    max_alias_id = max(max_alias_id, num)
                except (ValueError, IndexError):
                    pass

    for canonical_id, duplicate_ids, method, reason in consolidation_groups:
        for dup_id in duplicate_ids:
            dup_name = company_id_to_name[dup_id]
            if dup_name.lower() not in existing_aliases:
                max_alias_id += 1
                new_aliases.append({
                    "alias_id": f"alias_{max_alias_id:04d}",
                    "entity_type": "company",
                    "entity_id": canonical_id,
                    "alias_text": dup_name,
                })
                existing_aliases.add(dup_name.lower())

    # Write updated holdings
    holdings.to_csv(silver / "fact_reported_holding.csv", index=False)

    # Write updated aliases
    if new_aliases:
        new_aliases_df = pd.DataFrame(new_aliases)
        aliases = pd.concat([aliases, new_aliases_df], ignore_index=True)
        aliases.to_csv(alias_path, index=False)

    # Write consolidation log
    log_entries = []
    timestamp = datetime.datetime.now().isoformat()
    for canonical_id, duplicate_ids, method, reason in consolidation_groups:
        canonical_name = company_id_to_name[canonical_id]
        for dup_id in duplicate_ids:
            dup_name = company_id_to_name[dup_id]
            log_entries.append({
                "timestamp": timestamp,
                "action": "company_consolidation",
                "canonical_company_id": canonical_id,
                "canonical_company_name": canonical_name,
                "duplicate_company_id": dup_id,
                "duplicate_company_name": dup_name,
                "method": method,
                "reason": reason,
            })

    if log_entries:
        log_df = pd.DataFrame(log_entries)
        log_path = gold / "entity_resolution_log.csv"
        if log_path.exists():
            existing_log = pd.read_csv(log_path)
            log_df = pd.concat([existing_log, log_df], ignore_index=True)
        log_df.to_csv(log_path, index=False)

    # ============================================================
    # PRINT SUMMARY
    # ============================================================
    print("=" * 70)
    print("SAFE COMPANY CONSOLIDATION - RESULTS")
    print("=" * 70)

    # Group stats by method
    method_counts = {"normalized": 0, "token_overlap": 0, "first_entity": 0}
    for _, duplicate_ids, method, _ in consolidation_groups:
        method_counts[method] += len(duplicate_ids)

    print(f"\nGROUPS CONSOLIDATED: {len(consolidation_groups)}")
    print(f"  - Normalized matches:    {method_counts['normalized']}")
    print(f"  - Token overlap (>=0.90): {method_counts['token_overlap']}")
    print(f"  - First entity matches:  {method_counts['first_entity']}")

    print(f"\nHOLDINGS UPDATED: {holdings_updated:,}")
    print(f"ALIASES ADDED:   {len(new_aliases)}")

    print(f"\n" + "-" * 70)
    print(f"FALSE POSITIVES SKIPPED: {len(skipped)}")
    print("-" * 70)

    # Group skipped by method
    skipped_by_method: dict[str, list[tuple[str, str]]] = {}
    for name, method, reason in skipped:
        if method not in skipped_by_method:
            skipped_by_method[method] = []
        skipped_by_method[method].append((name, reason))

    for method, items in skipped_by_method.items():
        print(f"\n{method.upper()} ({len(items)} skipped):")
        for name, reason in items[:5]:
            print(f"  SKIP: {reason}")
            if len(name) < 60:
                print(f"        {name}")
        if len(items) > 5:
            print(f"  ... and {len(items) - 5} more")

    print(f"\n" + "-" * 70)
    print("CONSOLIDATION DETAILS:")
    print("-" * 70)
    for canonical_id, duplicate_ids, method, reason in consolidation_groups[:15]:
        canonical_name = company_id_to_name[canonical_id]
        print(f"\n[{method}] {reason}")
        print(f"  CANONICAL: '{canonical_name}'")
        for dup_id in duplicate_ids:
            dup_name = company_id_to_name[dup_id]
            print(f"  <- '{dup_name}'")

    if len(consolidation_groups) > 15:
        print(f"\n... and {len(consolidation_groups) - 15} more groups")

    print("\n" + "=" * 70)
    print("FILES UPDATED:")
    print(f"  - {silver / 'fact_reported_holding.csv'}")
    print(f"  - {silver / 'dim_entity_alias.csv'}")
    print(f"  - {gold / 'entity_resolution_log.csv'}")
    print("=" * 70)

    return {
        "groups_consolidated": len(consolidation_groups),
        "holdings_updated": holdings_updated,
        "aliases_added": len(new_aliases),
        "skipped": len(skipped),
        "consolidation_map": consolidation_map,
    }


def consolidate_company_duplicates(dry_run: bool = True) -> dict:
    """
    [DEPRECATED] Use consolidate_company_duplicates_safe() instead.

    This version has looser matching rules that can cause false positives.
    Kept for reference only.
    """
    root = _repo_root()
    silver = root / "data" / "silver"

    companies = _read_csv(silver / "dim_company.csv")
    holdings = _read_csv(silver / "fact_reported_holding.csv")

    company_id_to_name: dict[str, str] = {}
    for _, row in companies.iterrows():
        company_id = str(row["company_id"])
        company_id_to_name[company_id] = str(row["company_name"])

    # Build consolidation mapping: old_company_id -> canonical_company_id
    consolidation_map: dict[str, str] = {}

    # 1. Group by normalized name
    normalized_groups: dict[str, list[str]] = {}
    for company_id, company_name in company_id_to_name.items():
        normalized = _normalize_name(company_name)
        if normalized:
            if normalized not in normalized_groups:
                normalized_groups[normalized] = []
            normalized_groups[normalized].append(company_id)

    for normalized, company_ids in normalized_groups.items():
        if len(company_ids) > 1:
            # Sort by company_id to get consistent canonical choice
            company_ids_sorted = sorted(company_ids)
            canonical = company_ids_sorted[0]
            for cid in company_ids_sorted[1:]:
                consolidation_map[cid] = canonical

    # 2. Token overlap matches (only if not already mapped)
    token_groups: dict[str, tuple[set[str], str]] = {}
    for company_id, company_name in company_id_to_name.items():
        if company_id not in consolidation_map:
            normalized = _normalize_name(company_name)
            tokens = set(normalized.split()) if normalized else set()
            if tokens and len(tokens) >= 2:
                token_groups[company_id] = (tokens, normalized)

    processed: set[str] = set()
    for cid1, (tokens1, norm1) in token_groups.items():
        if cid1 in processed:
            continue
        similar_group = [cid1]
        for cid2, (tokens2, norm2) in token_groups.items():
            if cid1 == cid2 or cid2 in processed:
                continue
            if _jaccard_similarity(tokens1, tokens2) >= 0.70:
                similar_group.append(cid2)
                processed.add(cid2)
        if len(similar_group) > 1:
            similar_group_sorted = sorted(similar_group)
            canonical = similar_group_sorted[0]
            for cid in similar_group_sorted[1:]:
                if cid not in consolidation_map:
                    consolidation_map[cid] = canonical
        processed.add(cid1)

    # 3. First entity matches
    for company_id, company_name in company_id_to_name.items():
        if company_id in consolidation_map:
            continue
        first_entity = _extract_first_entity(company_name)
        if first_entity.lower() != company_name.lower():
            first_normalized = _normalize_name(first_entity)
            if first_normalized:
                # Find a matching company
                for other_id, other_name in company_id_to_name.items():
                    if other_id == company_id or other_id in consolidation_map:
                        continue
                    other_normalized = _normalize_name(other_name)
                    if other_normalized == first_normalized:
                        # Map the multi-entity name to the simpler name
                        consolidation_map[company_id] = other_id
                        break

    # Count holdings affected
    holdings_affected = 0
    holdings_by_method: dict[str, int] = {"normalized": 0, "token_overlap": 0, "first_entity": 0}

    for _, row in holdings.iterrows():
        current_company_id = row.get("company_id")
        if not _is_null(current_company_id):
            cid = str(current_company_id)
            if cid in consolidation_map:
                holdings_affected += 1

    print("=" * 70)
    print(f"Company Consolidation Analysis {'(DRY RUN)' if dry_run else ''}")
    print("=" * 70)
    print(f"Total companies: {len(company_id_to_name):,}")
    print(f"Companies to consolidate: {len(consolidation_map):,}")
    print(f"Holdings affected: {holdings_affected:,}")
    print()

    if not dry_run:
        # Actually update the holdings
        for idx, row in holdings.iterrows():
            current_company_id = row.get("company_id")
            if not _is_null(current_company_id):
                cid = str(current_company_id)
                if cid in consolidation_map:
                    holdings.at[idx, "company_id"] = consolidation_map[cid]

        holdings.to_csv(silver / "fact_reported_holding.csv", index=False)
        print(f"Updated holdings written to: {silver / 'fact_reported_holding.csv'}")

    return {
        "companies_consolidated": len(consolidation_map),
        "holdings_affected": holdings_affected,
        "consolidation_map": consolidation_map,
    }


def find_company_duplicates() -> list[dict]:
    """
    Find potential duplicate companies in dim_company using normalization methods.

    This identifies company records that may refer to the same underlying company
    but have different name variants (e.g., "Aptean, Inc." vs "Aptean Acquiror, Inc.").

    Returns:
        List of duplicate groups, each containing company_ids and names that match.
    """
    root = _repo_root()
    silver = root / "data" / "silver"

    companies = _read_csv(silver / "dim_company.csv")

    # Build lookup structures
    company_id_to_name: dict[str, str] = {}
    for _, row in companies.iterrows():
        company_id = str(row["company_id"])
        company_id_to_name[company_id] = str(row["company_name"])

    # Group companies by normalized name
    normalized_groups: dict[str, list[tuple[str, str]]] = {}  # normalized -> [(company_id, original_name)]
    for company_id, company_name in company_id_to_name.items():
        normalized = _normalize_name(company_name)
        if normalized:
            if normalized not in normalized_groups:
                normalized_groups[normalized] = []
            normalized_groups[normalized].append((company_id, company_name))

    # Find groups with multiple entries (duplicates)
    duplicate_groups = []
    for normalized, members in normalized_groups.items():
        if len(members) > 1:
            duplicate_groups.append({
                "normalized_name": normalized,
                "members": members,
                "count": len(members),
            })

    # Also check for token overlap matches between different normalized groups
    token_groups: dict[str, tuple[set[str], str, list[tuple[str, str]]]] = {}
    for normalized, members in normalized_groups.items():
        if len(members) == 1:  # Only single entries
            tokens = set(normalized.split())
            if len(tokens) >= 2:
                token_groups[normalized] = (tokens, normalized, members)

    token_overlap_duplicates = []
    processed_pairs: set[tuple[str, str]] = set()
    for norm1, (tokens1, _, members1) in token_groups.items():
        for norm2, (tokens2, _, members2) in token_groups.items():
            if norm1 >= norm2:  # Avoid duplicate pairs
                continue
            pair_key = (norm1, norm2)
            if pair_key in processed_pairs:
                continue

            similarity = _jaccard_similarity(tokens1, tokens2)
            if similarity >= 0.70:
                processed_pairs.add(pair_key)
                token_overlap_duplicates.append({
                    "similarity": similarity,
                    "group1": members1,
                    "group2": members2,
                })

    # Also try first entity extraction on multi-entity names
    first_entity_duplicates = []
    single_entity_normalized: dict[str, list[tuple[str, str]]] = {}

    for company_id, company_name in company_id_to_name.items():
        first_entity = _extract_first_entity(company_name)
        if first_entity.lower() != company_name.lower():
            # This is a multi-entity name
            first_normalized = _normalize_name(first_entity)
            if first_normalized:
                # Check if there's a single-entity company with this name
                for other_id, other_name in company_id_to_name.items():
                    if other_id == company_id:
                        continue
                    other_normalized = _normalize_name(other_name)
                    if other_normalized == first_normalized:
                        first_entity_duplicates.append({
                            "multi_entity": (company_id, company_name),
                            "first_entity_extracted": first_entity,
                            "matches": (other_id, other_name),
                        })
                        break

    print("=" * 70)
    print("Company Duplicate Analysis")
    print("=" * 70)

    # Report normalized duplicates
    print(f"\n1. NORMALIZED DUPLICATES: {len(duplicate_groups)} groups found")
    print("   (Companies that normalize to the same name)")
    for group in sorted(duplicate_groups, key=lambda x: -x["count"])[:10]:
        print(f"\n   Normalized: '{group['normalized_name']}'")
        for company_id, company_name in group["members"]:
            print(f"     - {company_id[:8]}... '{company_name}'")

    # Report token overlap duplicates
    print(f"\n2. TOKEN OVERLAP DUPLICATES: {len(token_overlap_duplicates)} pairs found")
    print("   (Companies with Jaccard similarity >= 0.70)")
    for pair in sorted(token_overlap_duplicates, key=lambda x: -x["similarity"])[:10]:
        print(f"\n   Similarity: {pair['similarity']:.2f}")
        for cid, name in pair["group1"]:
            print(f"     - '{name}'")
        for cid, name in pair["group2"]:
            print(f"     - '{name}'")

    # Report first entity duplicates
    print(f"\n3. FIRST ENTITY DUPLICATES: {len(first_entity_duplicates)} matches found")
    print("   (Multi-entity names where first entity matches another company)")
    for match in first_entity_duplicates[:10]:
        print(f"\n   Multi-entity: '{match['multi_entity'][1]}'")
        print(f"   First entity: '{match['first_entity_extracted']}'")
        print(f"   Matches:      '{match['matches'][1]}'")

    print("\n" + "=" * 70)
    total = len(duplicate_groups) + len(token_overlap_duplicates) + len(first_entity_duplicates)
    print(f"TOTAL POTENTIAL DUPLICATES: {total}")
    print("=" * 70)

    return duplicate_groups + token_overlap_duplicates + first_entity_duplicates


def run_full_analysis() -> None:
    """
    Run a comprehensive analysis showing resolution statistics before and after.
    """
    root = _repo_root()
    silver = root / "data" / "silver"

    holdings = _read_csv(silver / "fact_reported_holding.csv")
    companies = _read_csv(silver / "dim_company.csv")

    # Count current state
    total_holdings = len(holdings)
    resolved_before = sum(1 for _, row in holdings.iterrows() if not _is_null(row.get("company_id")))
    unresolved_before = total_holdings - resolved_before

    print("=" * 70)
    print("ENTITY RESOLUTION - COMPREHENSIVE ANALYSIS")
    print("=" * 70)

    print("\n" + "=" * 70)
    print("CURRENT STATE (BEFORE)")
    print("=" * 70)
    print(f"Total holdings:     {total_holdings:,}")
    print(f"Resolved:           {resolved_before:,} ({resolved_before/total_holdings*100:.1f}%)")
    print(f"Unresolved:         {unresolved_before:,} ({unresolved_before/total_holdings*100:.1f}%)")
    print(f"Total companies:    {len(companies):,}")

    print("\n" + "=" * 70)
    print("STEP 1: RESOLVE UNRESOLVED HOLDINGS")
    print("=" * 70)
    resolve_entities(verbose=True)

    # Reload to get updated state
    holdings = _read_csv(silver / "fact_reported_holding.csv")
    resolved_after_step1 = sum(1 for _, row in holdings.iterrows() if not _is_null(row.get("company_id")))
    new_resolved = resolved_after_step1 - resolved_before

    print(f"\nNew holdings resolved: {new_resolved:,}")

    print("\n" + "=" * 70)
    print("STEP 2: IDENTIFY DUPLICATE COMPANIES")
    print("=" * 70)
    find_company_duplicates()

    print("\n" + "=" * 70)
    print("STEP 3: CONSOLIDATION ANALYSIS (DRY RUN)")
    print("=" * 70)
    result = consolidate_company_duplicates(dry_run=True)

    print("\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)
    print(f"Holdings resolved by new methods:    {new_resolved:,}")
    print(f"Duplicate company groups identified: {result['companies_consolidated']:,}")
    print(f"Holdings that could be consolidated: {result['holdings_affected']:,}")
    print()
    print("Resolution Methods Added:")
    print("  1. NORMALIZED MATCH (0.90): Strips suffixes, punctuation, connectors")
    print("  2. TOKEN OVERLAP (0.80):   Jaccard similarity >= 0.70 on tokens")
    print("  3. FIRST ENTITY (0.75):    Extracts first company from multi-entity names")


def main() -> None:
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--analyze":
        analyze_potential_matches()
    elif len(sys.argv) > 1 and sys.argv[1] == "--duplicates":
        find_company_duplicates()
    elif len(sys.argv) > 1 and sys.argv[1] == "--consolidate":
        consolidate_company_duplicates_safe()
    elif len(sys.argv) > 1 and sys.argv[1] == "--consolidate-unsafe":
        consolidate_company_duplicates(dry_run=False)
    elif len(sys.argv) > 1 and sys.argv[1] == "--full":
        run_full_analysis()
    else:
        resolve_entities(verbose=True)


if __name__ == "__main__":
    main()
