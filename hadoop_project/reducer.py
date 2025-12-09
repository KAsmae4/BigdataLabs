#!/usr/bin/env python3
import sys

current = None
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, val = line.split('\t', 1)
    val = int(val)
    if key == current:
        count += val
    else:
        if current is not None:
            print(f"{current}\t{count}")
        current = key
        count = val

if current is not None:
    print(f"{current}\t{count}")
