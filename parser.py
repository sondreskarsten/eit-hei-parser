"""Parse EIT-HEI projects and resolve Norwegian partner orgnrs.

Reads project + partner_institution data, builds a
(project_slug, partner_name) → orgnr mapping via name matching
against the enheter snapshot.  EIT-HEI has no VAT field, so
resolution starts at name matching.

Resolution cascade:
    1. Exact uppercase match against enheter navn
    2. If country taxonomy indicates Norway
    3. Unresolved → logged separately

LUAS: ``(project_id, partner_institution_id)`` — one participation
per project per partner institution.
"""

import hashlib


def build_partner_lookup(partner_terms):
    """Build a {term_id: term_dict} lookup from partner_institution terms."""
    return {t["id"]: t for t in partner_terms}


def content_hash(project_slug, partner_name, acf):
    """Stable hash of project-partner participation state."""
    tracked = [
        project_slug,
        partner_name,
        str(acf.get("phase1_amount", "")),
        str(acf.get("phase2_amount", "")),
        str(acf.get("call_text", "")),
        str(acf.get("project_strand", "")),
    ]
    return hashlib.sha256("|".join(tracked).encode()).hexdigest()[:16]


KNOWN_NORWEGIAN_INSTITUTIONS = {
    "UIT ARCTIC UNIVERSITY OF NORWAY": "970422528",
    "UIT THE ARCTIC UNIVERSITY OF NORWAY": "970422528",
    "UNIVERSITETET I TROMSØ - NORGES ARKTISKE UNIVERSITET": "970422528",
    "NTNU": "974767880",
    "NORGES TEKNISK-NATURVITENSKAPELIGE UNIVERSITET NTNU": "974767880",
    "UNIVERSITY OF OSLO": "971035854",
    "UNIVERSITETET I OSLO": "971035854",
    "UNIVERSITY OF BERGEN": "874789542",
    "UNIVERSITETET I BERGEN": "874789542",
    "SINTEF": "950037687",
    "STIFTELSEN NORSK INSTITUTT FOR NATURFORSKNING NINA": "950037687",
    "NMBU": "969159570",
    "NORGES MILJØ- OG BIOVITENSKAPELIGE UNIVERSITET": "969159570",
    "OSLOMET": "954391631",
    "OSLOMET - STORBYUNIVERSITETET": "954391631",
    "NHH": "974789647",
    "NORGES HANDELSHØYSKOLE": "974789647",
    "SIMULA RESEARCH LABORATORY AS": "984648855",
    "OCTA INSIGHT AS": "930841366",
}


def resolve_orgnr(partner_name, enheter_lookup=None):
    """Resolve a partner institution name to a Norwegian orgnr.

    Args:
        partner_name: Name from the partner_institution taxonomy.
        enheter_lookup: Optional ``{uppercase_name: orgnr}`` dict
            from the enheter snapshot.

    Returns:
        Tuple of ``(orgnr, method)`` or ``(None, None)``.
    """
    name_upper = partner_name.strip().upper()

    orgnr = KNOWN_NORWEGIAN_INSTITUTIONS.get(name_upper)
    if orgnr:
        return orgnr, "known_institution"

    if enheter_lookup:
        orgnr = enheter_lookup.get(name_upper)
        if orgnr:
            return orgnr, "name_exact"

    return None, None


def parse_projects(projects, partner_lookup, enheter_lookup=None):
    """Parse projects into per-orgnr participation rows.

    For each project, iterates its ``partner_institution`` taxonomy IDs,
    resolves names to orgnrs, and produces one row per resolved partner.

    Args:
        projects: List of WP project dicts.
        partner_lookup: ``{term_id: term_dict}`` from partner_institution.
        enheter_lookup: Optional ``{uppercase_name: orgnr}`` dict.

    Returns:
        Tuple of ``(resolved_rows, unresolved_rows)``.
    """
    resolved = []
    unresolved = []

    for proj in projects:
        acf = proj.get("acf", {})
        slug = proj.get("slug", "")
        wp_id = proj.get("id", 0)
        title = proj.get("title", {}).get("rendered", "")
        partner_ids = proj.get("partner_institution", [])
        lead_partner_id = acf.get("lead_partner")

        for pid in partner_ids:
            term = partner_lookup.get(pid)
            if not term:
                continue

            partner_name = term.get("name", "")
            orgnr, method = resolve_orgnr(partner_name, enheter_lookup)
            is_lead = (pid == lead_partner_id)

            entry = {
                "orgnr": orgnr,
                "orgnr_resolution_method": method,
                "project_wp_id": wp_id,
                "project_slug": slug,
                "project_title": title,
                "partner_institution_id": pid,
                "partner_name": partner_name,
                "is_lead_partner": is_lead,
                "call_text": acf.get("call_text", ""),
                "project_strand": acf.get("project_strand", ""),
                "phase1_amount": acf.get("phase1_amount", ""),
                "phase2_amount": acf.get("phase2_amount", ""),
                "summary": acf.get("summary", ""),
                "project_modified": proj.get("modified", ""),
            "content_hash": content_hash(slug, partner_name, acf),
            }

            if orgnr:
                resolved.append(entry)
            else:
                unresolved.append(entry)

    return resolved, unresolved
