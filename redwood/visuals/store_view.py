#!/usr/bin/python

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

def storeHistogram(whitened, codebook, data_name, save_loc):
    plt.ioff()
    plt.clf()
    plt.hist(whitened, label=data_name)
    plt.hist(codebook, label="clusters")
    plt.legend()
    plt.savefig(save_loc)
