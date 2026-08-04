#!/usr/bin/env python3
"""Verify a ring all-reduce run by parsing BEFORE/AFTER blocks out of node logs.

Usage: python3 verify_allreduce.py node0.log node1.log node2.log node3.log
"""
import re
import sys


def parse_blocks(path):
    text = open(path).read()
    blocks = {}
    for label in ("BEFORE ALL-REDUCE", "AFTER ALL-REDUCE"):
        m = re.search(
            re.escape(f"=== Node ") + r"(\d+): " + re.escape(label) + r" ===\n(.*?)\n=====================",
            text,
            re.S,
        )
        if not m:
            sys.exit(f"{path}: could not find '{label}' block")
        rank = int(m.group(1))
        nums = [int(x) for x in m.group(2).split()]
        blocks[label] = (rank, nums)
    return blocks


def main():
    if len(sys.argv) < 3:
        sys.exit(f"Usage: {sys.argv[0]} <log1> <log2> ... <logN>")

    parsed = [parse_blocks(p) for p in sys.argv[1:]]
    parsed.sort(key=lambda b: b["BEFORE ALL-REDUCE"][0])

    ranks = [b["BEFORE ALL-REDUCE"][0] for b in parsed]
    if ranks != list(range(len(parsed))):
        sys.exit(f"Expected ranks 0..{len(parsed)-1}, got {ranks}")

    befores = [b["BEFORE ALL-REDUCE"][1] for b in parsed]
    afters = [b["AFTER ALL-REDUCE"][1] for b in parsed]

    expected = [sum(vals) for vals in zip(*befores)]

    ok = True

    for i in range(1, len(afters)):
        if afters[i] != afters[0]:
            print(f"FAIL: node {i}'s AFTER result differs from node 0's")
            print(f"  node0: {afters[0]}")
            print(f"  node{i}: {afters[i]}")
            ok = False

    if afters[0] != expected:
        print("FAIL: AFTER result does not equal sum of all BEFORE arrays")
        print(f"  expected (sum of BEFOREs): {expected}")
        print(f"  got (node0 AFTER):         {afters[0]}")
        ok = False

    if ok:
        print(f"PASS: all {len(parsed)} nodes agree, and result == sum of inputs")
        print(f"  result: {afters[0]}")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
