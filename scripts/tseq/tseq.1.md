---
title:      TSEQ
section:    1
header:     User Manual
footer:     tseq
date:       24 July 2021
---

# NAME
tseq - print a sequence of date/time values

# SYNOPSIS
**tseq** [*OPTION*] start stop

# DESCRIPTION
**tseq** is a simple python script that can be used to generate
time based parameters for shell scripts.

# OPTIONS
\-h 
: display help message

\--fs
: set field separator for pairs (default to space)

\-p \--pairs
: print subsequent pairs for ranges

\--ifmt IFMT
: set input format. accepted values are:

    * iso
    * iso_day
    * epoch
    * custom strftime format

\--ifmt IFMT
: set output format. refer to **ifmt** for values

\-t set interval size. accepted values are

    * years
    * months
    * days
    * minutes
    * seconds

# Examples

    tseq --ofmt epoch 2021-01-01 2021-02-01
