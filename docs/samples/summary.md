# Dataflow Gen1 to Gen2 migration assessment
Run `20261014T063000Z-e64d75` - generated 2026-10-14T06:30:01

## Inventory
| Capacities | Workspaces | With dataflows | Dataflows | Gen1 | Gen2 | Unknown |
|---|---|---|---|---|---|---|
| 3 | 36 | 35 | 132 | 117 | 15 | 0 |

## Migration progress
9 migrated, 114 remaining (7.3 % of 123); 1 Save-As failures. Estimated remaining effort: **274.6 person-days**, average complexity 24.2.

## Readiness of the remaining Gen1 dataflows
| Readiness | Dataflows | Share % |
|---|---|---|
| Ready | 27 | 23.7 |
| Needs Attention | 73 | 64.0 |
| Unsupported | 3 | 2.6 |
| Deferred | 11 | 9.6 |

## Predicted Upgrade Wizard assessment
| Wizard status | Dataflows | Share % |
|---|---|---|
| Ready to migrate | 29 | 25.4 |
| Needs Attention | 81 | 71.1 |
| Upgrade unavailable | 4 | 3.5 |

## Recommended path
| Path | Dataflows | Share % |
|---|---|---|
| FixThenReassess | 5 | 4.4 |
| None | 2 | 1.8 |
| Retire | 7 | 6.1 |
| UpgradeWizard | 100 | 87.7 |

## Complexity
| Band | Dataflows | Share % |
|---|---|---|
| Low | 45 | 39.5 |
| Medium | 59 | 51.8 |
| High | 9 | 7.9 |
| Very High | 1 | 0.9 |

## Migration waves
| Wave | Label | Dataflows | Workspaces | Ready | Needs Attention | Unsupported | Deferred | Effort (days) |
|---|---|---|---|---|---|---|---|---|
| 0 | Wave 0 - Pilot | 4 | 3 | 4 | 0 | 0 | 0 | 2.0 |
| 1 | Wave 1 - Quick wins | 16 | 13 | 16 | 0 | 0 | 0 | 8.0 |
| 2 | Wave 2 - Standard | 32 | 25 | 7 | 25 | 0 | 0 | 70.0 |
| 3 | Wave 3 - Complex or critical | 48 | 24 | 0 | 48 | 0 | 0 | 165.8 |
| 4 | Wave 4 - Remediate or rebuild | 3 | 1 | 0 | 0 | 3 | 0 | 20.2 |
| 99 | Backlog - Deferred | 11 | 10 | 0 | 0 | 0 | 11 | 8.6 |

## Most frequent findings
| Rule | Finding | Severity | Category | Dataflows | Share % |
|---|---|---|---|---|---|
| REB-001 | Downstream items to re-point after Save-As | Major | Lineage | 94 | 82.5 |
| SCH-002 | Schedule is copied disabled | Info | Refresh | 76 | 66.7 |
| LNK-001 | Contains linked entities | Major | Lineage | 27 | 23.7 |
| INC-001 | Incremental refresh configured | Major | Refresh | 16 | 14.0 |
| CMP-001 | Computed entities | Minor | Transform | 15 | 13.2 |
| LNK-002 | Read by other dataflows | Major | Lineage | 15 | 13.2 |
| GW-001 | Source through a data gateway | Minor | Connectivity | 14 | 12.3 |
| LNK-003 | Consumer uses the legacy PowerBI.Dataflows connector | Major | Lineage | 10 | 8.8 |
| ECE-001 | Enhanced compute engine enabled | Major | Compute | 9 | 7.9 |
| STA-001 | Stale and unused | Info | Governance | 9 | 7.9 |
| CAP-001 | Workspace not on a Fabric-capable capacity | Major | Capacity | 6 | 5.3 |
| DAT-001 | Definition not inspected | Minor | Data quality | 5 | 4.4 |
| DQ-001 | Downstream semantic model uses DirectQuery | Major | Lineage | 5 | 4.4 |
| PIP-001 | Referenced by data pipelines | Minor | Lineage | 5 | 4.4 |
| CON-001 | Unresolved data source connections | Blocker | Connectivity | 4 | 3.5 |
| STO-001 | Attached ADLS Gen2 storage (bring your own lake) | Major | Storage | 4 | 3.5 |
| AIX-001 | AI Insights or Azure ML functions | Minor | Transform | 3 | 2.6 |
| NAM-001 | Name contains unsupported characters | Minor | Governance | 3 | 2.6 |
| OWN-001 | Owner missing or disabled | Major | Governance | 3 | 2.6 |
| TRG-001 | External refresh triggers observed | Major | Refresh | 3 | 2.6 |
| NAM-002 | Duplicate name in the workspace | Minor | Governance | 2 | 1.8 |
| REF-001 | Refresh failing | Major | Refresh | 2 | 1.8 |
| LNK-004 | Upstream dataflow not inventoried | Major | Data quality | 1 | 0.9 |
| QRY-001 | More than 50 enabled queries | Major | Transform | 1 | 0.9 |

## Most complex dataflows
| Dataflow | Workspace | Score | Band | Readiness | Wave |
|---|---|---|---|---|---|
| Regulatory Calendar | Audit Reporting - Prod | 60.1 | Very High | Needs Attention | 3 |
| STG Intercompany Eliminations | Finance Reporting - Prod | 55.9 | High | Needs Attention | 3 |
| Suppliers | Procurement - Prod | 54.8 | High | Needs Attention | 3 |
| DIM Tax Codes | Finance Reporting - Prod | 49.8 | High | Needs Attention | 3 |
| FACT AR Aging | Real Estate - Prod | 48.0 | High | Needs Attention | 3 |
| FACT Cash Positions | Treasury - Prod | 47.6 | High | Unsupported | 4 |
| FACT GL Journal Entries | Treasury - Prod | 46.9 | High | Unsupported | 4 |
| Absences | HR Analytics - Prod | 46.5 | High | Needs Attention | 3 |
| STG NPS Survey | Customer 360 - Prod | 43.6 | High | Needs Attention | 3 |
| DIM Customer Master | Customer 360 - Prod | 40.6 | High | Needs Attention | 3 |
| DIM Calendar | Data Platform - Prod | 40.0 | Medium | Needs Attention | 3 |
| Service Desk Incidents | Data Platform - Prod | 39.5 | Medium | Needs Attention | 3 |
| STG Lead Times | Supply Chain - Prod | 39.1 | Medium | Needs Attention | 3 |
| Cost Centers | Finance Reporting - Dev | 39.1 | Medium | Needs Attention | 3 |
| Downtime Reasons | Plant Analytics - Prod | 38.7 | Medium | Needs Attention | 2 |
| Lead Scoring | Marketing Hub - Prod | 36.9 | Medium | Needs Attention | 3 |
| Contracts | Customer 360 - Prod | 36.5 | Medium | Needs Attention | 3 |
| Employees | Training Academy - Prod | 36.0 | Medium | Needs Attention | 3 |
| STG Campaigns | Campaign Performance - Prod | 35.8 | Medium | Needs Attention | 3 |
| DIM Org Hierarchy | Talent Acquisition - Prod | 35.7 | Medium | Needs Attention | 3 |
| Training Completion | Training Academy - Prod | 35.3 | Medium | Needs Attention | 2 |
| FACT Orders | Sales EMEA - Prod | 34.7 | Medium | Needs Attention | 3 |
| FACT Quality Defects | Quality Control - Prod | 34.0 | Medium | Needs Attention | 3 |
| STG Licensing | IT Operations - Prod | 33.8 | Medium | Needs Attention | 3 |
| Cost Centers | Budget & Planning - Prod | 33.4 | Medium | Needs Attention | 3 |

## Highest downstream impact
| Dataflow | Workspace | Models | Reports | Dashboards | Readiness |
|---|---|---|---|---|---|
| DIM Calendar | Data Platform - Prod | 8 | 15 | 3 | Needs Attention |
| STG Campaigns | Campaign Performance - Prod | 7 | 14 | 0 | Needs Attention |
| STG Intercompany Eliminations | Finance Reporting - Prod | 7 | 13 | 1 | Needs Attention |
| STG Licensing | IT Operations - Prod | 5 | 10 | 4 | Needs Attention |
| Cost Centers | Finance Reporting - Dev | 4 | 10 | 1 | Needs Attention |
| STG Policy Exceptions | Audit Reporting - Prod | 5 | 10 | 0 | Needs Attention |
| STG NPS Survey | Customer 360 - Prod | 5 | 10 | 0 | Needs Attention |
| DIM Employees | HR Analytics - Prod | 3 | 9 | 3 | Needs Attention |
| Asset Inventory | IT Operations - Prod | 4 | 9 | 2 | Needs Attention |
| Churn Signals | Customer 360 - Prod | 4 | 8 | 2 | Ready |
| FACT GL Journal Entries | Treasury - Prod | 3 | 7 | 3 | Unsupported |
| DIM Tax Codes | Budget & Planning - Prod | 3 | 8 | 2 | Needs Attention |
| STG Machine Telemetry Summary | Plant Analytics - Prod | 4 | 8 | 1 | Needs Attention |
| STG Lead Times | Supply Chain - Prod | 3 | 7 | 2 | Needs Attention |
| Downtime Reasons | Quality Control - Prod | 3 | 7 | 2 | Needs Attention |
| DIM Sales Reps | Product Catalog - Prod | 4 | 8 | 0 | Ready |
| Calendar | Self-Service BI Team | 3 | 7 | 1 | Needs Attention |
| DIM Loyalty Members | Sales EMEA - Prod | 3 | 7 | 1 | Needs Attention |
| Carrier Rates | Logistics Tracking - Prod | 3 | 8 | 0 | Needs Attention |
| Warehouse Locations | Procurement - Prod | 3 | 7 | 1 | Ready |
| Lead Scoring | Marketing Hub - Prod | 4 | 7 | 0 | Needs Attention |
| FACT Quality Defects | Quality Control - Prod | 3 | 5 | 3 | Needs Attention |
| Contracts | Customer 360 - Prod | 3 | 7 | 0 | Needs Attention |
| Regulatory Calendar | Audit Reporting - Prod | 3 | 7 | 0 | Needs Attention |
| DIM Price Lists | Sales EMEA - Prod | 3 | 7 | 0 | Needs Attention |

## Governance
| Environment | Dataflows | Share % |
|---|---|---|
| Dev | 9 | 7.9 |
| Prod | 98 | 86.0 |
| Test | 7 | 6.1 |

| Business criticality | Dataflows | Share % |
|---|---|---|
| High | 47 | 41.2 |
| Medium | 53 | 46.5 |
| Low | 14 | 12.3 |

| Capacity | Dataflows | Share % |
|---|---|---|
| (shared or unresolved) | 16 | 14.0 |
| contoso-fabric-dev | 13 | 11.4 |
| contoso-fabric-prod | 70 | 61.4 |
| contoso-premium-legacy | 15 | 13.2 |

## Refresh and connectivity
Failing refresh: 6; never refreshed: 24; scheduled: 76; average last duration: 1.3 min; dataflows behind a gateway: 14.

| Source category | Dataflows | Share % |
|---|---|---|
| Cloud storage | 3 | 1.5 |
| Database | 79 | 39.5 |
| Fabric | 4 | 2.0 |
| File | 19 | 9.5 |
| SaaS | 63 | 31.5 |
| Web | 32 | 16.0 |

## Data quality
| Assessment confidence | Dataflows | Share % |
|---|---|---|
| High | 82 | 71.9 |
| Medium | 31 | 27.2 |
| Low | 1 | 0.9 |

Definitions not inspected: 5; unknown generation: 0.
