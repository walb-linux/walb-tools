#!/bin/sh

version=`cat VERSION`
name=walb-tools
tarprefix=${name}_${version}/
tarfile=${name}_${version}.tar

git archive HEAD --format=tar --prefix $tarprefix -o $tarfile
tar f $tarfile --wildcards --delete '*/.gitignore'
tar rf $tarfile -h cybozulib/include/cybozu --transform "s|^|${tarprefix}|g"
tar rf $tarfile -h walb/include --transform "s|^|${tarprefix}|g"
gzip $tarfile
