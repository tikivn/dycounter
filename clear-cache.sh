find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf

find {where} -mtime +1 -type d -exec rm -rf {} \; 