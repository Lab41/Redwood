#!/usr/bin/python

import matplotlib.pyplot as plt


def showHistogram(whitened, codebook, data_name):

    plt.hist(whitened, label=data_name)
    plt.hist(codebook, label="clusters")
    plt.legend()
    plt.show()



