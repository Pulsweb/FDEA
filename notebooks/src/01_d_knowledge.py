# %% [markdown]
# ## 1.5 Rule catalog and domain knowledge
# The assessment is **data-driven**: every finding comes from a rule in this catalog (also persisted to
# `Assessment_RuleCatalog` for audit). Rules carry a separate effect for the two migration paths because the platform
# treats them differently:
#
# | Path | Mechanics | Id | Automation |
# |---|---|---|---|
# | **Upgrade Wizard** (preview) | in-place, per workspace, owner runs it | **kept** | UI only |
# | **Save As** (`saveAsNativeArtifact`, preview) | creates a Gen2 CI/CD copy; incremental refresh, enhanced compute, DirectQuery, AI insights, attached storage are not copied | **new** (consumers must be re-pointed) | REST API (Microsoft accelerator) |
#
# `Verified` rules come from Microsoft documentation (wizard assessment table, Gen1/Gen2 differences, Gen2 limits);
# `Heuristic` rules are detections that infer a documented limitation from indirect signals - tune them with the
# `wizard_status_actual` feedback column of the governance overrides.

# %% tags=["registry"]
import dataclasses as _dc

RULES_VERSION = "2026.10.1"


@_dc.dataclass(frozen=True)
class Rule:
    rule_id: str
    name: str
    category: str
    severity: str          # Blocker / Major / Minor / Info
    impact_wizard: str     # Unsupported / NeedsAttention / Deferred / None
    impact_saveas: str
    weight: int
    effort_days: float
    detection: str
    remediation: str
    confidence: str        # Verified / Heuristic
    enabled: bool = True


_RULES = [
    ("CAP-001", "Workspace not on a Fabric-capable capacity", "Capacity", "Major", "NeedsAttention", "NeedsAttention", 40, 0.5,
     "Workspace has no dedicated capacity or its SKU cannot host Fabric items (Pro, PPU, A/EM SKUs)",
     "Assign the workspace to a Fabric (F or trial) or Premium capacity first: Dataflow Gen2 is not available on Pro or PPU workspaces.", "Verified"),
    ("CAP-002", "Capacity not resolvable", "Capacity", "Info", "None", "None", 0, 0.0,
     "capacity_id is set but absent from the capacity APIs (service principals only see some capacities)",
     "Resolve the capacity manually and confirm it can host Fabric items.", "Heuristic"),
    ("STO-001", "Attached ADLS Gen2 storage (bring your own lake)", "Storage", "Major", "NeedsAttention", "Unsupported", 60, 2.0,
     "modelUrl points to a customer dfs.core.windows.net account, or the workspace has a dataflow storage account",
     "Wizard: configure an ADLS Gen2 data destination on the upgraded dataflow. Save-As does not move attached-storage data (CDM folders, BYOL): keep on Gen1 or redesign.", "Verified"),
    ("ECE-001", "Enhanced compute engine enabled", "Compute", "Major", "NeedsAttention", "NeedsAttention", 35, 1.5,
     "computeEngineBehavior is On",
     "DirectQuery over dataflows does not exist in Gen2: move DirectQuery consumers to Import or Direct Lake or read the destination tables directly.", "Verified"),
    ("ECE-002", "Enhanced compute engine set to Optimized", "Compute", "Info", "None", "None", 0, 0.0,
     "computeEngineBehavior is Optimized (default)", "No action: Gen2 uses its own staging compute.", "Heuristic"),
    ("DQ-001", "Downstream semantic model uses DirectQuery", "Lineage", "Major", "NeedsAttention", "NeedsAttention", 45, 2.0,
     "A semantic model reading the dataflow has targetStorageMode DirectQuery",
     "Reconfigure the semantic model to Import or Direct Lake: DirectQuery over dataflows is not supported in Gen2.", "Verified"),
    ("INC-001", "Incremental refresh configured", "Refresh", "Major", "NeedsAttention", "NeedsAttention", 30, 1.0,
     "An entity has an incremental refresh policy (pbi:refreshPolicy)",
     "Incremental settings are not carried over: define a supporting destination (Lakehouse, Warehouse or Azure SQL Database) and recreate the policy before the first refresh. The source must fold and expose a Date/DateTime column.", "Verified"),
    ("LNK-001", "Contains linked entities", "Lineage", "Major", "NeedsAttention", "NeedsAttention", 25, 1.0,
     "ReferenceEntity / referenceModels, PowerPlatform.Dataflows navigation or Scanner upstreamDataflows",
     "Cascading refresh through linked entities is not supported in Gen2: upgrade upstream dataflows first and orchestrate refreshes with a pipeline or schedule.", "Verified"),
    ("LNK-002", "Read by other dataflows", "Lineage", "Major", "NeedsAttention", "NeedsAttention", 20, 0.5,
     "Downstream dataflow edges exist",
     "After the upgrade refresh this dataflow, then open and save each downstream dataflow to rebind it.", "Verified"),
    ("LNK-003", "Consumer uses the legacy PowerBI.Dataflows connector", "Lineage", "Major", "NeedsAttention", "NeedsAttention", 25, 0.5,
     "M text of a downstream dataflow or semantic model contains PowerBI.Dataflows(",
     "Switch consumers to PowerPlatform.Dataflows: the legacy connector cannot read Gen2 items and the wizard does not detect it.", "Verified"),
    ("LNK-004", "Upstream dataflow not inventoried", "Data quality", "Major", "NeedsAttention", "NeedsAttention", 20, 0.25,
     "Lineage references an upstream dataflow that is not in the inventory (deleted, outside the scan scope or no access)",
     "Locate the upstream dataflow or fix the broken link: unresolved references make the wizard report Upgrade unavailable.", "Heuristic"),
    ("CMP-001", "Computed entities", "Transform", "Minor", "NeedsAttention", "NeedsAttention", 15, 0.5,
     "An entity query references another loaded entity of the same dataflow",
     "Gen2 has no computed-entity engine: validate results and use staged queries or destinations.", "Heuristic"),
    ("QRY-001", "More than 50 enabled queries", "Transform", "Major", "NeedsAttention", "NeedsAttention", 50, 3.0,
     "More than 50 enabled (loadEnabled) queries",
     "Gen2 supports at most 50 queries with staging or a destination: split the dataflow BEFORE upgrading.", "Verified"),
    ("QRY-002", "Close to the 50-query limit", "Transform", "Info", "None", "None", 0, 0.0,
     "40 to 50 enabled queries (already reflected in the technical complexity component)", "Plan headroom or split the dataflow.", "Heuristic"),
    ("CON-001", "Unresolved data source connections", "Connectivity", "Blocker", "Unsupported", "NeedsAttention", 60, 1.0,
     "The Scanner reports misconfigured datasource usages",
     "Recreate or rebind the failing connections: the wizard reports Upgrade unavailable until they resolve.", "Verified"),
    ("GW-001", "Source through a data gateway", "Connectivity", "Minor", "NeedsAttention", "NeedsAttention", 15, 0.5,
     "A source carries a gateway id",
     "Verify the gateway runs a supported version (latest six releases) and validate or recreate the Fabric connection; OAuth2 credentials on gateway connections fail after one hour.", "Verified"),
    ("OWN-001", "Owner missing or disabled", "Governance", "Major", "NeedsAttention", "NeedsAttention", 20, 0.25,
     "Owner is empty or the account is disabled (Microsoft Graph enrichment)",
     "The wizard must be run by the dataflow owner: use Take over, then confirm the Gen1 dataflow refreshes successfully.", "Verified"),
    ("NAM-001", "Name contains unsupported characters", "Governance", "Minor", "NeedsAttention", "None", 5, 0.1,
     "Name has characters other than letters, digits, whitespace and ()[]{}+-=_#",
     "The wizard strips them automatically; rename first to control the result.", "Verified"),
    ("NAM-002", "Duplicate name in the workspace", "Governance", "Minor", "NeedsAttention", "None", 5, 0.1,
     "Another dataflow of the workspace has the same name", "The wizard prefixes Migrated: rename first.", "Verified"),
    ("SCH-001", "Schedule exceeds 48 runs per day", "Refresh", "Minor", "NeedsAttention", "NeedsAttention", 10, 0.25,
     "schedule_runs_per_day is greater than 48", "Gen2 supports up to 48 scheduled refreshes per day.", "Verified"),
    ("SCH-002", "Schedule is copied disabled", "Refresh", "Info", "None", "NeedsAttention", 5, 0.1,
     "The dataflow has an enabled refresh schedule",
     "Save-As copies the schedule in a disabled state: re-enable it after validation.", "Verified"),
    ("TRG-001", "External refresh triggers observed", "Refresh", "Major", "NeedsAttention", "NeedsAttention", 20, 0.5,
     "Refresh history shows on-demand or API-triggered runs",
     "Power Automate flows, scripts and APIs calling the Power BI refresh API must move to the Fabric Job Scheduler API (and to the new id after Save-As).", "Heuristic"),
    ("PIP-001", "Referenced by data pipelines", "Lineage", "Minor", "None", "NeedsAttention", 15, 0.25,
     "A pipeline RefreshDataFlow activity references the dataflow",
     "After Save-As re-point each pipeline activity to the new dataflow id.", "Verified"),
    ("REB-001", "Downstream items to re-point after Save-As", "Lineage", "Major", "None", "NeedsAttention", 30, 0.25,
     "Downstream semantic models, reports or dataflows exist",
     "Save-As creates a new id: re-point every downstream item (effort scales with their number). The wizard keeps the id.", "Verified"),
    ("AIX-001", "AI Insights or Azure ML functions", "Transform", "Minor", "NeedsAttention", "NeedsAttention", 15, 1.0,
     "M uses AI.*, CognitiveServices.* or AzureML.* functions",
     "AI insights are not copied: rebuild with the Fabric AI prompt in Dataflow Gen2.", "Verified"),
    ("REF-001", "Refresh failing", "Refresh", "Major", "Deferred", "Deferred", 0, 0.0,
     "The latest N refreshes all failed",
     "Fix the failing refresh first: a healthy Gen1 baseline is needed to validate the migration.", "Heuristic"),
    ("STA-001", "Stale and unused", "Governance", "Info", "Deferred", "Deferred", 0, 0.0,
     "No successful refresh for N days and no downstream consumers",
     "Decommission candidate: confirm with the owner before spending migration effort.", "Heuristic"),
    ("SEQ-001", "Blocked by an unsupported or deferred upstream dataflow", "Lineage", "Major", "Deferred", "Deferred", 0, 0.0,
     "An upstream dataflow is Unsupported or Deferred",
     "Resolve the upstream dataflow first: upstream dataflows are migrated before their consumers.", "Verified"),
    ("DAT-001", "Definition not inspected", "Data quality", "Minor", "NeedsAttention", "NeedsAttention", 10, 0.0,
     "The definition could not be collected (no access, not attempted or failed)",
     "Grant the service principal workspace access or enable deeper collection, then re-run; validate with the Upgrade Wizard assessment.", "Heuristic"),
]
RULES = {r[0]: Rule(*r) for r in _RULES}
IMPACTS = ("Unsupported", "NeedsAttention", "Deferred", "None")


def rule_catalog_rows():
    return [{"rule_id": r.rule_id, "name": r.name, "category": r.category, "severity": r.severity, "impact_wizard": r.impact_wizard,
             "impact_saveas": r.impact_saveas, "weight": r.weight, "effort_days": r.effort_days, "detection": r.detection,
             "remediation": r.remediation, "confidence": r.confidence, "enabled": r.enabled, "version": RULES_VERSION}
            for r in RULES.values()]

# %%
# Datasource classification (Scanner datasourceType / M function prefix) -> category and relative migration weight.
SOURCE_CATEGORY_RULES = [
    ("Dataflow", ("powerplatform.dataflows", "powerbi.dataflows", "dataflow")),
    ("Fabric", ("lakehouse", "warehouse", "onelake", "fabric")),
    ("File", ("file", "folder", "excel", "csv", "text", "json", "xml", "pdf")),
    ("Cloud storage", ("azureblob", "blob", "azurestorage", "datalake", "adls", "amazons3", "s3", "googlecloudstorage")),
    ("SaaS", ("sharepoint", "salesforce", "dynamics", "dataverse", "commondataservice", "googleanalytics", "servicenow", "zendesk",
              "marketo", "hubspot", "jira", "github", "smartsheet")),
    ("Web", ("web", "odata", "rest", "http", "api")),
    ("Database", ("sql", "oracle", "postgre", "mysql", "odbc", "oledb", "snowflake", "teradata", "db2", "sap", "hana", "analysisservices",
                  "kusto", "dataexplorer", "databricks", "synapse", "redshift", "bigquery", "mongo", "cosmos", "access", "informix", "sybase")),
]
SOURCE_COMPLEXITY = {"Database": 1.0, "Fabric": 0.5, "Cloud storage": 1.0, "Dataflow": 1.0, "File": 1.5, "SaaS": 1.5, "Web": 2.0, "Other": 2.0}
FILE_BASED_CATEGORIES = {"File"}


def categorize_source(datasource_type):
    text = (datasource_type or "").lower().replace(" ", "")
    for category, needles in SOURCE_CATEGORY_RULES:
        if any(n in text for n in needles):
            return category
    return "Other"


def classify_sku(sku):
    """Return (capacity_kind, is_fabric_capable). Capability of P SKUs depends on the tenant enabling Fabric."""
    s = (sku or "").upper().strip()
    if not s:
        return "Unknown", None
    if s.startswith("FT") or "TRIAL" in s:
        return "Trial", True
    if re.fullmatch(r"F\d+", s):
        return "Fabric F", True
    if re.fullmatch(r"P\d+", s):
        return "Premium P", True
    if re.fullmatch(r"(EM|A)\d+", s):
        return "Embedded", False
    return "Unknown", None


_BOUND = r"(?:^|[\s_\-\.\[\(/])"
_TAIL = r"(?:[\s_\-\.\]\)/]|$)"
DEFAULT_ENV_PATTERNS = {
    "Prod": rf"{_BOUND}(prd|prod|production|live){_TAIL}",
    "Test": rf"{_BOUND}(tst|test|uat|qa|sit|stg|stage|staging|preprod|pre-prod|ppd){_TAIL}",
    "Dev": rf"{_BOUND}(dev|development|sandbox|poc){_TAIL}",
}


def detect_environment(name, patterns=None):
    """Environment from a workspace name; ambiguous or unmatched names are Unknown."""
    found = [env for env, pattern in (patterns or DEFAULT_ENV_PATTERNS).items() if re.search(pattern, name or "", re.IGNORECASE)]
    return found[0] if len(found) == 1 else "Unknown"


def stage_environment(order, stage_count):
    """Map a deployment pipeline stage to an environment (first = Dev, last = Prod, others = Test)."""
    if order is None or not stage_count or stage_count < 2:
        return "Unknown"
    return "Dev" if order == 0 else ("Prod" if order == stage_count - 1 else "Test")


ALLOWED_NAME_CHARS = re.compile(r"^[\w\s()\[\]{}+\-=#]+$", re.UNICODE)
