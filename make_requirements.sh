#!/bin/bash

pipreqs --ignore docs/ --print . | sort -f > requirements.txt
sed -i '/nesta/d' ./requirements.txt
