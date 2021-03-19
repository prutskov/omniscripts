#!/bin/bash
for f in *.meta; do
  grep -qc q1, $f && ln -s ${f/.meta/} ${f/out.meta/}q1 && ln -s $f ${f/out.meta/}q1.meta
  grep -qc q2, $f && ln -s ${f/.meta/} ${f/out.meta/}q2 && ln -s $f ${f/out.meta/}q2.meta
  grep -qc q3, $f && ln -s ${f/.meta/} ${f/out.meta/}q3 && ln -s $f ${f/out.meta/}q3.meta
  grep -qc q4, $f && ln -s ${f/.meta/} ${f/out.meta/}q4 && ln -s $f ${f/out.meta/}q4.meta
done
