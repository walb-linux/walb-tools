#!/usr/bin/python2

import walblib
import sys


def main():
    line = sys.stdin.readline()
    while line:
        diff = walblib.Diff()
        diff.parse(line.strip())
        print diff.genFileName()
        line = sys.stdin.readline()


if __name__ == '__main__':
    main()
