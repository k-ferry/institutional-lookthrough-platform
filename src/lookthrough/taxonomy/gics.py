"""
GICS (Global Industry Classification Standard) taxonomy module.

The GICS hierarchy has 4 levels:
- Sector (11 sectors, 2-digit codes: 10, 15, 20, ...)
- Industry Group (25 groups, 4-digit codes: 1010, 1510, 2010, ...)
- Industry (74 industries, 6-digit codes: 101010, 151010, ...)
- Sub-Industry (163 sub-industries, 8-digit codes: 10101010, 15101010, ...)

Source: MSCI/S&P GICS structure (2023 revision)
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Optional

import pandas as pd


def _repo_root() -> Path:
    # src/lookthrough/taxonomy/gics.py -> repo root is 4 parents up
    return Path(__file__).resolve().parents[3]


def _deterministic_uuid(gics_code: str) -> str:
    """Generate deterministic UUID from GICS code using namespace UUID."""
    namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # UUID namespace
    return str(uuid.uuid5(namespace, f"gics_{gics_code}"))


# =============================================================================
# GICS HIERARCHY DATA
# Complete GICS structure as of 2023 revision
# =============================================================================

GICS_SECTORS = [
    {"code": "10", "name": "Energy"},
    {"code": "15", "name": "Materials"},
    {"code": "20", "name": "Industrials"},
    {"code": "25", "name": "Consumer Discretionary"},
    {"code": "30", "name": "Consumer Staples"},
    {"code": "35", "name": "Health Care"},
    {"code": "40", "name": "Financials"},
    {"code": "45", "name": "Information Technology"},
    {"code": "50", "name": "Communication Services"},
    {"code": "55", "name": "Utilities"},
    {"code": "60", "name": "Real Estate"},
]

GICS_INDUSTRY_GROUPS = [
    # Energy
    {"code": "1010", "name": "Energy", "parent_code": "10"},
    # Materials
    {"code": "1510", "name": "Materials", "parent_code": "15"},
    # Industrials
    {"code": "2010", "name": "Capital Goods", "parent_code": "20"},
    {"code": "2020", "name": "Commercial & Professional Services", "parent_code": "20"},
    {"code": "2030", "name": "Transportation", "parent_code": "20"},
    # Consumer Discretionary
    {"code": "2510", "name": "Automobiles & Components", "parent_code": "25"},
    {"code": "2520", "name": "Consumer Durables & Apparel", "parent_code": "25"},
    {"code": "2530", "name": "Consumer Services", "parent_code": "25"},
    {"code": "2550", "name": "Consumer Discretionary Distribution & Retail", "parent_code": "25"},
    # Consumer Staples
    {"code": "3010", "name": "Consumer Staples Distribution & Retail", "parent_code": "30"},
    {"code": "3020", "name": "Food, Beverage & Tobacco", "parent_code": "30"},
    {"code": "3030", "name": "Household & Personal Products", "parent_code": "30"},
    # Health Care
    {"code": "3510", "name": "Health Care Equipment & Services", "parent_code": "35"},
    {"code": "3520", "name": "Pharmaceuticals, Biotechnology & Life Sciences", "parent_code": "35"},
    # Financials
    {"code": "4010", "name": "Banks", "parent_code": "40"},
    {"code": "4020", "name": "Financial Services", "parent_code": "40"},
    {"code": "4030", "name": "Insurance", "parent_code": "40"},
    # Information Technology
    {"code": "4510", "name": "Software & Services", "parent_code": "45"},
    {"code": "4520", "name": "Technology Hardware & Equipment", "parent_code": "45"},
    {"code": "4530", "name": "Semiconductors & Semiconductor Equipment", "parent_code": "45"},
    # Communication Services
    {"code": "5010", "name": "Telecommunication Services", "parent_code": "50"},
    {"code": "5020", "name": "Media & Entertainment", "parent_code": "50"},
    # Utilities
    {"code": "5510", "name": "Utilities", "parent_code": "55"},
    # Real Estate
    {"code": "6010", "name": "Equity Real Estate Investment Trusts (REITs)", "parent_code": "60"},
    {"code": "6020", "name": "Real Estate Management & Development", "parent_code": "60"},
]

GICS_INDUSTRIES = [
    # Energy (1010)
    {"code": "101010", "name": "Energy Equipment & Services", "parent_code": "1010"},
    {"code": "101020", "name": "Oil, Gas & Consumable Fuels", "parent_code": "1010"},
    # Materials (1510)
    {"code": "151010", "name": "Chemicals", "parent_code": "1510"},
    {"code": "151020", "name": "Construction Materials", "parent_code": "1510"},
    {"code": "151030", "name": "Containers & Packaging", "parent_code": "1510"},
    {"code": "151040", "name": "Metals & Mining", "parent_code": "1510"},
    {"code": "151050", "name": "Paper & Forest Products", "parent_code": "1510"},
    # Capital Goods (2010)
    {"code": "201010", "name": "Aerospace & Defense", "parent_code": "2010"},
    {"code": "201020", "name": "Building Products", "parent_code": "2010"},
    {"code": "201030", "name": "Construction & Engineering", "parent_code": "2010"},
    {"code": "201040", "name": "Electrical Equipment", "parent_code": "2010"},
    {"code": "201050", "name": "Industrial Conglomerates", "parent_code": "2010"},
    {"code": "201060", "name": "Machinery", "parent_code": "2010"},
    {"code": "201070", "name": "Trading Companies & Distributors", "parent_code": "2010"},
    # Commercial & Professional Services (2020)
    {"code": "202010", "name": "Commercial Services & Supplies", "parent_code": "2020"},
    {"code": "202020", "name": "Professional Services", "parent_code": "2020"},
    # Transportation (2030)
    {"code": "203010", "name": "Air Freight & Logistics", "parent_code": "2030"},
    {"code": "203020", "name": "Passenger Airlines", "parent_code": "2030"},
    {"code": "203030", "name": "Marine Transportation", "parent_code": "2030"},
    {"code": "203040", "name": "Ground Transportation", "parent_code": "2030"},
    {"code": "203050", "name": "Transportation Infrastructure", "parent_code": "2030"},
    # Automobiles & Components (2510)
    {"code": "251010", "name": "Automobile Components", "parent_code": "2510"},
    {"code": "251020", "name": "Automobiles", "parent_code": "2510"},
    # Consumer Durables & Apparel (2520)
    {"code": "252010", "name": "Household Durables", "parent_code": "2520"},
    {"code": "252020", "name": "Leisure Products", "parent_code": "2520"},
    {"code": "252030", "name": "Textiles, Apparel & Luxury Goods", "parent_code": "2520"},
    # Consumer Services (2530)
    {"code": "253010", "name": "Hotels, Restaurants & Leisure", "parent_code": "2530"},
    {"code": "253020", "name": "Diversified Consumer Services", "parent_code": "2530"},
    # Consumer Discretionary Distribution & Retail (2550)
    {"code": "255010", "name": "Distributors", "parent_code": "2550"},
    {"code": "255020", "name": "Broadline Retail", "parent_code": "2550"},
    {"code": "255030", "name": "Specialty Retail", "parent_code": "2550"},
    # Consumer Staples Distribution & Retail (3010)
    {"code": "301010", "name": "Consumer Staples Distribution & Retail", "parent_code": "3010"},
    # Food, Beverage & Tobacco (3020)
    {"code": "302010", "name": "Beverages", "parent_code": "3020"},
    {"code": "302020", "name": "Food Products", "parent_code": "3020"},
    {"code": "302030", "name": "Tobacco", "parent_code": "3020"},
    # Household & Personal Products (3030)
    {"code": "303010", "name": "Household Products", "parent_code": "3030"},
    {"code": "303020", "name": "Personal Care Products", "parent_code": "3030"},
    # Health Care Equipment & Services (3510)
    {"code": "351010", "name": "Health Care Equipment & Supplies", "parent_code": "3510"},
    {"code": "351020", "name": "Health Care Providers & Services", "parent_code": "3510"},
    {"code": "351030", "name": "Health Care Technology", "parent_code": "3510"},
    # Pharmaceuticals, Biotechnology & Life Sciences (3520)
    {"code": "352010", "name": "Biotechnology", "parent_code": "3520"},
    {"code": "352020", "name": "Pharmaceuticals", "parent_code": "3520"},
    {"code": "352030", "name": "Life Sciences Tools & Services", "parent_code": "3520"},
    # Banks (4010)
    {"code": "401010", "name": "Banks", "parent_code": "4010"},
    # Financial Services (4020)
    {"code": "402010", "name": "Financial Services", "parent_code": "4020"},
    {"code": "402020", "name": "Consumer Finance", "parent_code": "4020"},
    {"code": "402030", "name": "Capital Markets", "parent_code": "4020"},
    {"code": "402040", "name": "Mortgage Real Estate Investment Trusts (REITs)", "parent_code": "4020"},
    # Insurance (4030)
    {"code": "403010", "name": "Insurance", "parent_code": "4030"},
    # Software & Services (4510)
    {"code": "451010", "name": "IT Services", "parent_code": "4510"},
    {"code": "451020", "name": "Software", "parent_code": "4510"},
    # Technology Hardware & Equipment (4520)
    {"code": "452010", "name": "Communications Equipment", "parent_code": "4520"},
    {"code": "452020", "name": "Technology Hardware, Storage & Peripherals", "parent_code": "4520"},
    {"code": "452030", "name": "Electronic Equipment, Instruments & Components", "parent_code": "4520"},
    # Semiconductors & Semiconductor Equipment (4530)
    {"code": "453010", "name": "Semiconductors & Semiconductor Equipment", "parent_code": "4530"},
    # Telecommunication Services (5010)
    {"code": "501010", "name": "Diversified Telecommunication Services", "parent_code": "5010"},
    {"code": "501020", "name": "Wireless Telecommunication Services", "parent_code": "5010"},
    # Media & Entertainment (5020)
    {"code": "502010", "name": "Media", "parent_code": "5020"},
    {"code": "502020", "name": "Entertainment", "parent_code": "5020"},
    {"code": "502030", "name": "Interactive Media & Services", "parent_code": "5020"},
    # Utilities (5510)
    {"code": "551010", "name": "Electric Utilities", "parent_code": "5510"},
    {"code": "551020", "name": "Gas Utilities", "parent_code": "5510"},
    {"code": "551030", "name": "Multi-Utilities", "parent_code": "5510"},
    {"code": "551040", "name": "Water Utilities", "parent_code": "5510"},
    {"code": "551050", "name": "Independent Power and Renewable Electricity Producers", "parent_code": "5510"},
    # Equity Real Estate Investment Trusts (REITs) (6010)
    {"code": "601010", "name": "Diversified REITs", "parent_code": "6010"},
    {"code": "601020", "name": "Industrial REITs", "parent_code": "6010"},
    {"code": "601025", "name": "Hotel & Resort REITs", "parent_code": "6010"},
    {"code": "601030", "name": "Office REITs", "parent_code": "6010"},
    {"code": "601040", "name": "Health Care REITs", "parent_code": "6010"},
    {"code": "601050", "name": "Residential REITs", "parent_code": "6010"},
    {"code": "601060", "name": "Retail REITs", "parent_code": "6010"},
    {"code": "601070", "name": "Specialized REITs", "parent_code": "6010"},
    # Real Estate Management & Development (6020)
    {"code": "602010", "name": "Real Estate Management & Development", "parent_code": "6020"},
]

GICS_SUB_INDUSTRIES = [
    # Energy Equipment & Services (101010)
    {"code": "10101010", "name": "Oil & Gas Drilling", "parent_code": "101010"},
    {"code": "10101020", "name": "Oil & Gas Equipment & Services", "parent_code": "101010"},
    # Oil, Gas & Consumable Fuels (101020)
    {"code": "10102010", "name": "Integrated Oil & Gas", "parent_code": "101020"},
    {"code": "10102020", "name": "Oil & Gas Exploration & Production", "parent_code": "101020"},
    {"code": "10102030", "name": "Oil & Gas Refining & Marketing", "parent_code": "101020"},
    {"code": "10102040", "name": "Oil & Gas Storage & Transportation", "parent_code": "101020"},
    {"code": "10102050", "name": "Coal & Consumable Fuels", "parent_code": "101020"},
    # Chemicals (151010)
    {"code": "15101010", "name": "Commodity Chemicals", "parent_code": "151010"},
    {"code": "15101020", "name": "Diversified Chemicals", "parent_code": "151010"},
    {"code": "15101030", "name": "Fertilizers & Agricultural Chemicals", "parent_code": "151010"},
    {"code": "15101040", "name": "Industrial Gases", "parent_code": "151010"},
    {"code": "15101050", "name": "Specialty Chemicals", "parent_code": "151010"},
    # Construction Materials (151020)
    {"code": "15102010", "name": "Construction Materials", "parent_code": "151020"},
    # Containers & Packaging (151030)
    {"code": "15103010", "name": "Metal, Glass & Plastic Containers", "parent_code": "151030"},
    {"code": "15103020", "name": "Paper & Plastic Packaging Products & Materials", "parent_code": "151030"},
    # Metals & Mining (151040)
    {"code": "15104010", "name": "Aluminum", "parent_code": "151040"},
    {"code": "15104020", "name": "Diversified Metals & Mining", "parent_code": "151040"},
    {"code": "15104025", "name": "Copper", "parent_code": "151040"},
    {"code": "15104030", "name": "Gold", "parent_code": "151040"},
    {"code": "15104040", "name": "Precious Metals & Minerals", "parent_code": "151040"},
    {"code": "15104045", "name": "Silver", "parent_code": "151040"},
    {"code": "15104050", "name": "Steel", "parent_code": "151040"},
    # Paper & Forest Products (151050)
    {"code": "15105010", "name": "Forest Products", "parent_code": "151050"},
    {"code": "15105020", "name": "Paper Products", "parent_code": "151050"},
    # Aerospace & Defense (201010)
    {"code": "20101010", "name": "Aerospace & Defense", "parent_code": "201010"},
    # Building Products (201020)
    {"code": "20102010", "name": "Building Products", "parent_code": "201020"},
    # Construction & Engineering (201030)
    {"code": "20103010", "name": "Construction & Engineering", "parent_code": "201030"},
    # Electrical Equipment (201040)
    {"code": "20104010", "name": "Electrical Components & Equipment", "parent_code": "201040"},
    {"code": "20104020", "name": "Heavy Electrical Equipment", "parent_code": "201040"},
    # Industrial Conglomerates (201050)
    {"code": "20105010", "name": "Industrial Conglomerates", "parent_code": "201050"},
    # Machinery (201060)
    {"code": "20106010", "name": "Construction Machinery & Heavy Transportation Equipment", "parent_code": "201060"},
    {"code": "20106015", "name": "Agricultural & Farm Machinery", "parent_code": "201060"},
    {"code": "20106020", "name": "Industrial Machinery & Supplies & Components", "parent_code": "201060"},
    # Trading Companies & Distributors (201070)
    {"code": "20107010", "name": "Trading Companies & Distributors", "parent_code": "201070"},
    # Commercial Services & Supplies (202010)
    {"code": "20201010", "name": "Commercial Printing", "parent_code": "202010"},
    {"code": "20201050", "name": "Environmental & Facilities Services", "parent_code": "202010"},
    {"code": "20201060", "name": "Office Services & Supplies", "parent_code": "202010"},
    {"code": "20201070", "name": "Diversified Support Services", "parent_code": "202010"},
    {"code": "20201080", "name": "Security & Alarm Services", "parent_code": "202010"},
    # Professional Services (202020)
    {"code": "20202010", "name": "Human Resource & Employment Services", "parent_code": "202020"},
    {"code": "20202020", "name": "Research & Consulting Services", "parent_code": "202020"},
    {"code": "20202030", "name": "Data Processing & Outsourced Services", "parent_code": "202020"},
    # Air Freight & Logistics (203010)
    {"code": "20301010", "name": "Air Freight & Logistics", "parent_code": "203010"},
    # Passenger Airlines (203020)
    {"code": "20302010", "name": "Passenger Airlines", "parent_code": "203020"},
    # Marine Transportation (203030)
    {"code": "20303010", "name": "Marine Transportation", "parent_code": "203030"},
    # Ground Transportation (203040)
    {"code": "20304010", "name": "Rail Transportation", "parent_code": "203040"},
    {"code": "20304020", "name": "Trucking", "parent_code": "203040"},
    {"code": "20304030", "name": "Cargo Ground Transportation", "parent_code": "203040"},
    # Transportation Infrastructure (203050)
    {"code": "20305010", "name": "Airport Services", "parent_code": "203050"},
    {"code": "20305020", "name": "Highways & Railtracks", "parent_code": "203050"},
    {"code": "20305030", "name": "Marine Ports & Services", "parent_code": "203050"},
    # Automobile Components (251010)
    {"code": "25101010", "name": "Automotive Parts & Equipment", "parent_code": "251010"},
    {"code": "25101020", "name": "Tires & Rubber", "parent_code": "251010"},
    # Automobiles (251020)
    {"code": "25102010", "name": "Automobile Manufacturers", "parent_code": "251020"},
    {"code": "25102020", "name": "Motorcycle Manufacturers", "parent_code": "251020"},
    # Household Durables (252010)
    {"code": "25201010", "name": "Consumer Electronics", "parent_code": "252010"},
    {"code": "25201020", "name": "Home Furnishings", "parent_code": "252010"},
    {"code": "25201030", "name": "Homebuilding", "parent_code": "252010"},
    {"code": "25201040", "name": "Household Appliances", "parent_code": "252010"},
    {"code": "25201050", "name": "Housewares & Specialties", "parent_code": "252010"},
    # Leisure Products (252020)
    {"code": "25202010", "name": "Leisure Products", "parent_code": "252020"},
    # Textiles, Apparel & Luxury Goods (252030)
    {"code": "25203010", "name": "Apparel, Accessories & Luxury Goods", "parent_code": "252030"},
    {"code": "25203020", "name": "Footwear", "parent_code": "252030"},
    {"code": "25203030", "name": "Textiles", "parent_code": "252030"},
    # Hotels, Restaurants & Leisure (253010)
    {"code": "25301010", "name": "Casinos & Gaming", "parent_code": "253010"},
    {"code": "25301020", "name": "Hotels, Resorts & Cruise Lines", "parent_code": "253010"},
    {"code": "25301030", "name": "Leisure Facilities", "parent_code": "253010"},
    {"code": "25301040", "name": "Restaurants", "parent_code": "253010"},
    # Diversified Consumer Services (253020)
    {"code": "25302010", "name": "Education Services", "parent_code": "253020"},
    {"code": "25302020", "name": "Specialized Consumer Services", "parent_code": "253020"},
    # Distributors (255010)
    {"code": "25501010", "name": "Distributors", "parent_code": "255010"},
    # Broadline Retail (255020)
    {"code": "25502010", "name": "Broadline Retail", "parent_code": "255020"},
    # Specialty Retail (255030)
    {"code": "25503010", "name": "Apparel Retail", "parent_code": "255030"},
    {"code": "25503020", "name": "Computer & Electronics Retail", "parent_code": "255030"},
    {"code": "25503030", "name": "Home Improvement Retail", "parent_code": "255030"},
    {"code": "25503040", "name": "Other Specialty Retail", "parent_code": "255030"},
    {"code": "25503050", "name": "Automotive Retail", "parent_code": "255030"},
    {"code": "25503060", "name": "Homefurnishing Retail", "parent_code": "255030"},
    # Consumer Staples Distribution & Retail (301010)
    {"code": "30101010", "name": "Drug Retail", "parent_code": "301010"},
    {"code": "30101020", "name": "Food Distributors", "parent_code": "301010"},
    {"code": "30101030", "name": "Food Retail", "parent_code": "301010"},
    {"code": "30101040", "name": "Consumer Staples Merchandise Retail", "parent_code": "301010"},
    # Beverages (302010)
    {"code": "30201010", "name": "Brewers", "parent_code": "302010"},
    {"code": "30201020", "name": "Distillers & Vintners", "parent_code": "302010"},
    {"code": "30201030", "name": "Soft Drinks & Non-alcoholic Beverages", "parent_code": "302010"},
    # Food Products (302020)
    {"code": "30202010", "name": "Agricultural Products & Services", "parent_code": "302020"},
    {"code": "30202030", "name": "Packaged Foods & Meats", "parent_code": "302020"},
    # Tobacco (302030)
    {"code": "30203010", "name": "Tobacco", "parent_code": "302030"},
    # Household Products (303010)
    {"code": "30301010", "name": "Household Products", "parent_code": "303010"},
    # Personal Care Products (303020)
    {"code": "30302010", "name": "Personal Care Products", "parent_code": "303020"},
    # Health Care Equipment & Supplies (351010)
    {"code": "35101010", "name": "Health Care Equipment", "parent_code": "351010"},
    {"code": "35101020", "name": "Health Care Supplies", "parent_code": "351010"},
    # Health Care Providers & Services (351020)
    {"code": "35102010", "name": "Health Care Distributors", "parent_code": "351020"},
    {"code": "35102015", "name": "Health Care Services", "parent_code": "351020"},
    {"code": "35102020", "name": "Health Care Facilities", "parent_code": "351020"},
    {"code": "35102030", "name": "Managed Health Care", "parent_code": "351020"},
    # Health Care Technology (351030)
    {"code": "35103010", "name": "Health Care Technology", "parent_code": "351030"},
    # Biotechnology (352010)
    {"code": "35201010", "name": "Biotechnology", "parent_code": "352010"},
    # Pharmaceuticals (352020)
    {"code": "35202010", "name": "Pharmaceuticals", "parent_code": "352020"},
    # Life Sciences Tools & Services (352030)
    {"code": "35203010", "name": "Life Sciences Tools & Services", "parent_code": "352030"},
    # Banks (401010)
    {"code": "40101010", "name": "Diversified Banks", "parent_code": "401010"},
    {"code": "40101015", "name": "Regional Banks", "parent_code": "401010"},
    # Financial Services (402010)
    {"code": "40201010", "name": "Diversified Financial Services", "parent_code": "402010"},
    {"code": "40201020", "name": "Multi-Sector Holdings", "parent_code": "402010"},
    {"code": "40201030", "name": "Specialized Finance", "parent_code": "402010"},
    {"code": "40201040", "name": "Commercial & Residential Mortgage Finance", "parent_code": "402010"},
    {"code": "40201050", "name": "Transaction & Payment Processing Services", "parent_code": "402010"},
    # Consumer Finance (402020)
    {"code": "40202010", "name": "Consumer Finance", "parent_code": "402020"},
    # Capital Markets (402030)
    {"code": "40203010", "name": "Asset Management & Custody Banks", "parent_code": "402030"},
    {"code": "40203020", "name": "Investment Banking & Brokerage", "parent_code": "402030"},
    {"code": "40203030", "name": "Diversified Capital Markets", "parent_code": "402030"},
    {"code": "40203040", "name": "Financial Exchanges & Data", "parent_code": "402030"},
    # Mortgage Real Estate Investment Trusts (REITs) (402040)
    {"code": "40204010", "name": "Mortgage REITs", "parent_code": "402040"},
    # Insurance (403010)
    {"code": "40301010", "name": "Insurance Brokers", "parent_code": "403010"},
    {"code": "40301020", "name": "Life & Health Insurance", "parent_code": "403010"},
    {"code": "40301030", "name": "Multi-line Insurance", "parent_code": "403010"},
    {"code": "40301040", "name": "Property & Casualty Insurance", "parent_code": "403010"},
    {"code": "40301050", "name": "Reinsurance", "parent_code": "403010"},
    # IT Services (451010)
    {"code": "45101010", "name": "IT Consulting & Other Services", "parent_code": "451010"},
    {"code": "45101020", "name": "Internet Services & Infrastructure", "parent_code": "451010"},
    # Software (451020)
    {"code": "45102010", "name": "Application Software", "parent_code": "451020"},
    {"code": "45102020", "name": "Systems Software", "parent_code": "451020"},
    # Communications Equipment (452010)
    {"code": "45201010", "name": "Communications Equipment", "parent_code": "452010"},
    # Technology Hardware, Storage & Peripherals (452020)
    {"code": "45202010", "name": "Technology Hardware, Storage & Peripherals", "parent_code": "452020"},
    # Electronic Equipment, Instruments & Components (452030)
    {"code": "45203010", "name": "Electronic Equipment & Instruments", "parent_code": "452030"},
    {"code": "45203015", "name": "Electronic Components", "parent_code": "452030"},
    {"code": "45203020", "name": "Electronic Manufacturing Services", "parent_code": "452030"},
    {"code": "45203030", "name": "Technology Distributors", "parent_code": "452030"},
    # Semiconductors & Semiconductor Equipment (453010)
    {"code": "45301010", "name": "Semiconductor Materials & Equipment", "parent_code": "453010"},
    {"code": "45301020", "name": "Semiconductors", "parent_code": "453010"},
    # Diversified Telecommunication Services (501010)
    {"code": "50101010", "name": "Alternative Carriers", "parent_code": "501010"},
    {"code": "50101020", "name": "Integrated Telecommunication Services", "parent_code": "501010"},
    # Wireless Telecommunication Services (501020)
    {"code": "50102010", "name": "Wireless Telecommunication Services", "parent_code": "501020"},
    # Media (502010)
    {"code": "50201010", "name": "Advertising", "parent_code": "502010"},
    {"code": "50201020", "name": "Broadcasting", "parent_code": "502010"},
    {"code": "50201030", "name": "Cable & Satellite", "parent_code": "502010"},
    {"code": "50201040", "name": "Publishing", "parent_code": "502010"},
    # Entertainment (502020)
    {"code": "50202010", "name": "Movies & Entertainment", "parent_code": "502020"},
    {"code": "50202020", "name": "Interactive Home Entertainment", "parent_code": "502020"},
    # Interactive Media & Services (502030)
    {"code": "50203010", "name": "Interactive Media & Services", "parent_code": "502030"},
    # Electric Utilities (551010)
    {"code": "55101010", "name": "Electric Utilities", "parent_code": "551010"},
    # Gas Utilities (551020)
    {"code": "55102010", "name": "Gas Utilities", "parent_code": "551020"},
    # Multi-Utilities (551030)
    {"code": "55103010", "name": "Multi-Utilities", "parent_code": "551030"},
    # Water Utilities (551040)
    {"code": "55104010", "name": "Water Utilities", "parent_code": "551040"},
    # Independent Power and Renewable Electricity Producers (551050)
    {"code": "55105010", "name": "Independent Power Producers & Energy Traders", "parent_code": "551050"},
    {"code": "55105020", "name": "Renewable Electricity", "parent_code": "551050"},
    # Diversified REITs (601010)
    {"code": "60101010", "name": "Diversified REITs", "parent_code": "601010"},
    # Industrial REITs (601020)
    {"code": "60102010", "name": "Industrial REITs", "parent_code": "601020"},
    # Hotel & Resort REITs (601025)
    {"code": "60102510", "name": "Hotel & Resort REITs", "parent_code": "601025"},
    # Office REITs (601030)
    {"code": "60103010", "name": "Office REITs", "parent_code": "601030"},
    # Health Care REITs (601040)
    {"code": "60104010", "name": "Health Care REITs", "parent_code": "601040"},
    # Residential REITs (601050)
    {"code": "60105010", "name": "Multi-Family Residential REITs", "parent_code": "601050"},
    {"code": "60105020", "name": "Single-Family Residential REITs", "parent_code": "601050"},
    # Retail REITs (601060)
    {"code": "60106010", "name": "Retail REITs", "parent_code": "601060"},
    # Specialized REITs (601070)
    {"code": "60107010", "name": "Other Specialized REITs", "parent_code": "601070"},
    {"code": "60107015", "name": "Self-Storage REITs", "parent_code": "601070"},
    {"code": "60107020", "name": "Telecom Tower REITs", "parent_code": "601070"},
    {"code": "60107030", "name": "Timber REITs", "parent_code": "601070"},
    {"code": "60107040", "name": "Data Center REITs", "parent_code": "601070"},
    # Real Estate Management & Development (602010)
    {"code": "60201010", "name": "Diversified Real Estate Activities", "parent_code": "602010"},
    {"code": "60201020", "name": "Real Estate Operating Companies", "parent_code": "602010"},
    {"code": "60201030", "name": "Real Estate Development", "parent_code": "602010"},
    {"code": "60201040", "name": "Real Estate Services", "parent_code": "602010"},
]


def get_gics_taxonomy() -> list[dict]:
    """
    Return the full GICS taxonomy as a flat list of dicts.

    Each dict has:
    - code: GICS numeric code (str)
    - name: Node name
    - parent_code: Parent GICS code (None for sectors)
    - level: sector, industry_group, industry, sub_industry
    """
    result = []

    # Sectors (level 1)
    for s in GICS_SECTORS:
        result.append({
            "code": s["code"],
            "name": s["name"],
            "parent_code": None,
            "level": "sector",
        })

    # Industry Groups (level 2)
    for ig in GICS_INDUSTRY_GROUPS:
        result.append({
            "code": ig["code"],
            "name": ig["name"],
            "parent_code": ig["parent_code"],
            "level": "industry_group",
        })

    # Industries (level 3)
    for ind in GICS_INDUSTRIES:
        result.append({
            "code": ind["code"],
            "name": ind["name"],
            "parent_code": ind["parent_code"],
            "level": "industry",
        })

    # Sub-Industries (level 4)
    for si in GICS_SUB_INDUSTRIES:
        result.append({
            "code": si["code"],
            "name": si["name"],
            "parent_code": si["parent_code"],
            "level": "sub_industry",
        })

    return result


def _build_code_to_uuid_map(taxonomy: list[dict]) -> dict[str, str]:
    """Build a mapping from GICS code to deterministic UUID."""
    return {node["code"]: _deterministic_uuid(node["code"]) for node in taxonomy}


def _build_path(node: dict, code_to_name: dict[str, str]) -> str:
    """Build the hierarchical path for a node."""
    parts = [node["name"]]
    parent_code = node.get("parent_code")

    while parent_code:
        parts.insert(0, code_to_name[parent_code])
        # Find parent's parent
        if len(parent_code) == 2:
            parent_code = None
        elif len(parent_code) == 4:
            parent_code = parent_code[:2]
        elif len(parent_code) == 6:
            parent_code = parent_code[:4]
        elif len(parent_code) == 8:
            parent_code = parent_code[:6]
        else:
            parent_code = None

    return "/" + "/".join(parts)


def _get_level_number(level: str) -> int:
    """Convert level name to numeric level."""
    return {"sector": 1, "industry_group": 2, "industry": 3, "sub_industry": 4}[level]


def write_gics_taxonomy_to_csv(output_path: Optional[Path] = None) -> Path:
    """
    Write the GICS taxonomy to dim_taxonomy_node.csv format.

    Schema:
    - taxonomy_node_id: deterministic UUID from gics_code
    - taxonomy_version_id: fixed UUID for GICS version
    - taxonomy_type: sector/industry_group/industry/sub_industry
    - node_name: GICS name
    - parent_node_id: UUID of parent node (empty for sectors)
    - path: hierarchical path like /Energy/Oil, Gas & Consumable Fuels
    - level: numeric level (1-4)
    - source: "gics"

    Returns the output path.
    """
    if output_path is None:
        output_path = _repo_root() / "data" / "silver" / "dim_taxonomy_node.csv"

    taxonomy = get_gics_taxonomy()
    code_to_uuid = _build_code_to_uuid_map(taxonomy)
    code_to_name = {node["code"]: node["name"] for node in taxonomy}

    # Fixed taxonomy version ID for GICS
    taxonomy_version_id = str(uuid.uuid5(
        uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
        "gics_2023"
    ))

    rows = []
    for node in taxonomy:
        parent_uuid = ""
        if node["parent_code"]:
            parent_uuid = code_to_uuid[node["parent_code"]]

        rows.append({
            "taxonomy_node_id": code_to_uuid[node["code"]],
            "taxonomy_version_id": taxonomy_version_id,
            "taxonomy_type": node["level"],
            "node_name": node["name"],
            "parent_node_id": parent_uuid,
            "path": _build_path(node, code_to_name),
            "level": _get_level_number(node["level"]),
            "source": "gics",
        })

    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    return output_path


def get_sub_industry_lookup() -> dict[str, dict]:
    """
    Return a lookup dict mapping sub_industry code to full hierarchy info.

    Used by map_to_gics to get all ancestor codes and names for a sub_industry.
    """
    taxonomy = get_gics_taxonomy()
    code_to_node = {n["code"]: n for n in taxonomy}

    lookup = {}
    for node in taxonomy:
        if node["level"] != "sub_industry":
            continue

        sub_code = node["code"]
        ind_code = node["parent_code"]
        ind_node = code_to_node[ind_code]
        ig_code = ind_node["parent_code"]
        ig_node = code_to_node[ig_code]
        sec_code = ig_node["parent_code"]
        sec_node = code_to_node[sec_code]

        lookup[sub_code] = {
            "gics_sector_code": sec_code,
            "gics_sector_name": sec_node["name"],
            "gics_industry_group_code": ig_code,
            "gics_industry_group_name": ig_node["name"],
            "gics_industry_code": ind_code,
            "gics_industry_name": ind_node["name"],
            "gics_sub_industry_code": sub_code,
            "gics_sub_industry_name": node["name"],
        }

    return lookup


if __name__ == "__main__":
    # When run directly, write the GICS taxonomy to CSV
    out_path = write_gics_taxonomy_to_csv()
    taxonomy = get_gics_taxonomy()

    print(f"Wrote GICS taxonomy to: {out_path}")
    print(f"  Sectors: {len([n for n in taxonomy if n['level'] == 'sector'])}")
    print(f"  Industry Groups: {len([n for n in taxonomy if n['level'] == 'industry_group'])}")
    print(f"  Industries: {len([n for n in taxonomy if n['level'] == 'industry'])}")
    print(f"  Sub-Industries: {len([n for n in taxonomy if n['level'] == 'sub_industry'])}")
    print(f"  Total: {len(taxonomy)}")
