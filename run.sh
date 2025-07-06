#!/bin/bash
pytest tests/
spark-submit programaprincipal.py data/*.json
