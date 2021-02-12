#!/bin/bash
for f in *.meta; do
  grep -qc q1, $f && ln -s ${f/.meta/} ${f/out.meta/}q1 && ln -s $f ${f/out.meta/}q1.meta
  grep -qc q11, $f && ln -s ${f/.meta/} ${f/out.meta/}q11 && ln -s $f ${f/out.meta/}q11.meta
  grep -qc q5, $f && ln -s ${f/.meta/} ${f/out.meta/}q5 && ln -s $f ${f/out.meta/}q5.meta
done
