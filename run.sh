#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Использование: $0 <YYYY-MM-DD>"
  exit 1
fi

DATE_ARG=$1

python3 scripts/scrapers/YT_scraper.py "$DATE_ARG" &
python3 scripts/scrapers/X_scraper.py "$DATE_ARG" &
python3 scripts/scrapers/IG_scraper.py "$DATE_ARG"

wait

python3 scripts/evaluation.py "$DATE_ARG"