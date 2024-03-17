# OS libraries required

## apt

    libsnappy-dev
    libmysqlclient-dev
    gnuplot
    graphviz

# Tex
To install Tex-live directly (helpful as you can update and install packages):

Instrucitons [here](https://www.tug.org/texlive/quickinstall.html).  Install as
follows

    sudo perl ./install-tl --scheme=basic

Add the latex path to the PATH variable, e.g.:

    /usr/local/texlive/2023/bin/x86_64-linux/

Install the following packages:

    booktabs
    eso-pic
    lastpage
    microtype
    etoolbox
    relax
    helvetic
    enumitem
    xcolor
    titlesec
    pgfgantt
    pgf
