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


def get_active_companies(companies_config: dict = None) -> dict:
    """Get only active companies from config."""
    if companies_config is None:
        companies_config = load_companies_config()
    return {
        k: v for k, v in companies_config.get('companies', {}).items()
        if v.get('is_active', False)
    }


def get_driver_hierarchy(sectors_config: dict = None) -> dict:
    """Get hierarchical driver weights."""
    if sectors_config is None:
        sectors_config = load_sectors_config()
    return sectors_config.get('driver_hierarchy', {
        'macro_weight': 0.20,
        'sector_weight': 0.55,
        'company_weight': 0.25,
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
