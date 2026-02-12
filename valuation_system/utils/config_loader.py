"""
Configuration Loader
Loads and validates all YAML/env configuration.
Single source of truth for accessing config values.
"""

import os
import logging

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_CONFIG_DIR = os.path.join(os.path.dirname(__file__), '..', 'config')
load_dotenv(os.path.join(_CONFIG_DIR, '.env'))


def load_sectors_config() -> dict:
    """Load sectors.yaml configuration."""
    path = os.path.join(_CONFIG_DIR, 'sectors.yaml')
    with open(path, 'r') as f:
        config = yaml.safe_load(f)
    logger.info(f"Loaded sectors config: {len(config.get('sectors', {}))} sectors")
    return config


def load_companies_config() -> dict:
    """Load companies.yaml configuration."""
    path = os.path.join(_CONFIG_DIR, 'companies.yaml')
    with open(path, 'r') as f:
        config = yaml.safe_load(f)
    logger.info(f"Loaded companies config: {len(config.get('companies', {}))} companies")
    return config


def get_active_sectors(sectors_config: dict = None) -> dict:
    """Get only active sectors from config."""
    if sectors_config is None:
        sectors_config = load_sectors_config()
    return {
        k: v for k, v in sectors_config.get('sectors', {}).items()
        if v.get('is_active', False)
    }


def get_active_companies(companies_config: dict = None, mysql_client=None, use_yaml_fallback: bool = True,
                         sort_by: str = 'priority') -> dict:
    """
    Get active companies from database or YAML fallback.

    Args:
        companies_config: Legacy YAML config (deprecated, for backward compatibility)
        mysql_client: ValuationMySQLClient instance (if None, uses YAML fallback)
        use_yaml_fallback: If True, fall back to YAML when DB fails or is None
        sort_by: Sort order - 'priority' (default), 'mcap' (market cap desc), 'symbol' (alphabetical)

    Returns:
        dict: Companies keyed by nse_symbol with configuration data
    """
    # Try database first if client provided
    if mysql_client is not None:
        try:
            # Determine ORDER BY clause based on sort_by
            if sort_by == 'mcap':
                order_by = "m.mcap DESC, ac.nse_symbol"
            elif sort_by == 'symbol':
                order_by = "ac.nse_symbol ASC"
            else:  # priority (default)
                order_by = "ac.priority ASC, ac.valuation_group, ac.nse_symbol"

            companies = mysql_client.query(f"""
                SELECT
                    ac.id, ac.company_id, ac.nse_symbol, ac.company_name,
                    ac.csv_name, ac.bse_code, ac.accord_code,
                    ac.valuation_group, ac.valuation_subgroup,
                    ac.cd_sector, ac.cd_industry,
                    ac.sector, ac.industry,
                    ac.valuation_frequency, ac.priority, ac.alpha_config_id,
                    ac.is_active, ac.added_date, ac.notes,
                    m.mcap
                FROM vs_active_companies ac
                LEFT JOIN mssdb.kbapp_marketscrip m ON ac.company_id = m.marketscrip_id
                WHERE ac.is_active = 1
                ORDER BY {order_by}
            """)

            if not companies:
                logger.warning("No active companies found in database")
                if use_yaml_fallback:
                    logger.info("Falling back to YAML configuration")
                    return get_active_companies_from_yaml(companies_config)
                return {}

            logger.info(f"Loaded {len(companies)} active companies from database")

            # Transform to expected format (dict keyed by nse_symbol)
            result = {}
            for company in companies:
                key = company['nse_symbol']
                config = {
                    'id': company['id'],
                    'company_id': company['company_id'],
                    'csv_name': company['csv_name'] or company['company_name'],
                    'nse_symbol': company['nse_symbol'],
                    'bse_code': company['bse_code'],
                    'accord_code': company['accord_code'],
                    'sector': company['valuation_group'],  # Primary: valuation_group
                    'valuation_group': company['valuation_group'],
                    'valuation_subgroup': company['valuation_subgroup'],
                    'cd_sector': company['cd_sector'],  # Original classification
                    'cd_industry': company['cd_industry'],
                    'legacy_sector': company['sector'],  # Old sector field
                    'legacy_industry': company['industry'],  # Old industry field
                    'is_active': True,
                    'valuation_frequency': company['valuation_frequency'],
                    'priority': company['priority'],
                    'added_date': str(company['added_date']) if company['added_date'] else None,
                    'notes': company['notes'],
                }

                # Load alpha config if exists
                if company['alpha_config_id']:
                    try:
                        alpha = mysql_client.query_one(
                            "SELECT * FROM vs_company_alpha_configs WHERE company_id = %s",
                            (company['company_id'],)
                        )
                        if alpha:
                            import json
                            config['alpha_thesis'] = {
                                'bull': alpha.get('thesis_bull', ''),
                                'bear': alpha.get('thesis_bear', ''),
                                'key_moat': alpha.get('thesis_key_moat', ''),
                            }
                            config['alpha_drivers'] = json.loads(alpha.get('alpha_drivers', '{}')) if isinstance(alpha.get('alpha_drivers'), str) else alpha.get('alpha_drivers', {})
                            config['sector_specific_overrides'] = json.loads(alpha.get('sector_overrides', '{}')) if isinstance(alpha.get('sector_overrides'), str) else alpha.get('sector_overrides', {})
                    except Exception as e:
                        logger.warning(f"Failed to load alpha config for {key}: {e}")

                result[key] = config

            return result

        except Exception as e:
            logger.error(f"Failed to load companies from database: {e}")
            if use_yaml_fallback:
                logger.info("Falling back to YAML configuration")
                return get_active_companies_from_yaml(companies_config)
            raise

    # Fallback to YAML if no mysql_client provided
    if use_yaml_fallback:
        return get_active_companies_from_yaml(companies_config)

    return {}


def get_active_companies_from_yaml(companies_config: dict = None) -> dict:
    """
    Legacy YAML loader - backward compatibility.

    Args:
        companies_config: Pre-loaded YAML config or None to load fresh

    Returns:
        dict: Companies keyed by company key (e.g., 'aether_industries')
    """
    if companies_config is None:
        companies_config = load_companies_config()
    return {
        k: v for k, v in companies_config.get('companies', {}).items()
        if v.get('is_active', False)
    }


def get_driver_hierarchy(sectors_config: dict = None) -> dict:
    """Get hierarchical driver weights (4-level: Macro/Group/Subgroup/Company)."""
    if sectors_config is None:
        sectors_config = load_sectors_config()
    return sectors_config.get('driver_hierarchy', {
        'macro_weight': 0.15,
        'group_weight': 0.20,       # valuation_group level
        'subgroup_weight': 0.35,    # valuation_subgroup level
        'company_weight': 0.30,
    })


def get_blend_weights() -> dict:
    """Get valuation blending weights from env."""
    return {
        'dcf': float(os.getenv('VALUATION_BLEND_DCF', 0.60)),
        'relative': float(os.getenv('VALUATION_BLEND_RELATIVE', 0.30)),
        'monte_carlo': float(os.getenv('VALUATION_BLEND_MC', 0.10)),
    }


def get_social_media_config(companies_config: dict = None) -> dict:
    """Get social media posting configuration."""
    if companies_config is None:
        companies_config = load_companies_config()
    return companies_config.get('social_media', {})
