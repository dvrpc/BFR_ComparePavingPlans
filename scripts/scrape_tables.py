
   
"""
scrape_packages.py
------------------
This script loops over every PDF within a given folder.
For each PDF, it extracts tabular data from every page

"""

from __future__ import annotations
from pathlib import Path
import tabula
import pandas as pd
from PyPDF2 import PdfFileReader
import env_vars as ev
from env_vars import ENGINE

