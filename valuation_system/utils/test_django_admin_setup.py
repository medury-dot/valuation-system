#!/usr/bin/env python3
"""
Test Django Admin Setup for News Watchlist
Verifies that the model appears under correct app section.
"""

import os
import sys
import django

# Setup Django
sys.path.insert(0, '/Users/ram/code/rag/machai/RAGApp')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rag_django.settings')
django.setup()

from django.contrib.admin import site
from valuation_system.models import VsNewsWatchlist
from valuation_system.admin import VsNewsWatchlistAdmin

print("=" * 80)
print("Django Admin Setup Test")
print("=" * 80)

# Check model registration
print("\n1. Model Registration:")
print("-" * 80)
if VsNewsWatchlist in site._registry:
    admin_class = site._registry[VsNewsWatchlist]
    print(f"✓ VsNewsWatchlist is registered")
    print(f"  Admin class: {admin_class.__class__.__name__}")
    print(f"  App label: {VsNewsWatchlist._meta.app_label}")
    print(f"  Verbose name: {VsNewsWatchlist._meta.verbose_name_plural}")
else:
    print("✗ VsNewsWatchlist is NOT registered")

# Check actions
print("\n2. Available Actions:")
print("-" * 80)
if VsNewsWatchlist in site._registry:
    admin_class = site._registry[VsNewsWatchlist]
    actions = admin_class.actions
    print(f"  Total actions: {len(actions)}")
    for action in actions:
        if hasattr(action, 'short_description'):
            desc = action.short_description
        elif isinstance(action, str):
            desc = action.replace('_', ' ').title()
        else:
            desc = str(action)
        print(f"    - {desc}")

# Check data
print("\n3. Current Watchlist Data:")
print("-" * 80)
from rag.models import KbappMarketscrip

watchlist = VsNewsWatchlist.objects.all()[:10]
print(f"  Total entries: {VsNewsWatchlist.objects.count()}")
print(f"  Enabled: {VsNewsWatchlist.objects.filter(is_enabled=True).count()}")
print(f"\n  Top 10:")
for entry in watchlist:
    try:
        company = KbappMarketscrip.objects.using('mssdb').get(marketscrip_id=entry.company_id)
        status = "✓" if entry.is_enabled else "✗"
        print(f"    {status} {company.symbol:12} {company.name[:40]:40} [{entry.priority}]")
    except:
        print(f"    ? ID:{entry.company_id:6} (company not found)")

# Check other admins have the action
print("\n4. Cross-Admin Actions:")
print("-" * 80)

from mssdb.admin import KbappMarketscripAdmin
from rag.admin import VsActiveCompaniesAdmin
from rag.models import VsActiveCompanies

# Check KbappMarketscripAdmin
kbapp_actions = [a for a in KbappMarketscripAdmin.actions if 'news' in str(a).lower()]
if kbapp_actions:
    print(f"✓ KbappMarketscripAdmin has news watchlist action: {kbapp_actions}")
else:
    print("✗ KbappMarketscripAdmin missing news watchlist action")

# Check VsActiveCompaniesAdmin
active_actions = [a for a in VsActiveCompaniesAdmin.actions if 'news' in str(a).lower()]
if active_actions:
    print(f"✓ VsActiveCompaniesAdmin has news watchlist action: {active_actions}")
else:
    print("✗ VsActiveCompaniesAdmin missing news watchlist action")

print("\n" + "=" * 80)
print("✓ Test Complete!")
print("=" * 80)
print("\nNext Steps:")
print("1. Start Django: cd /Users/ram/code/rag/machai/RAGApp && python manage.py runserver")
print("2. Visit: http://localhost:8000/admin/")
print("3. Look for 'VALUATION SYSTEM > News Watchlist' (not under RAG)")
print("4. Try actions in Market Scrip and Active Companies admins")
