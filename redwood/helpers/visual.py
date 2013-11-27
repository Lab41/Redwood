#!/usr/bin/env python
#
# Copyright (c) 2013 In-Q-Tel, Inc/Lab41, All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Created on 19 October 2013
@author: Lab41

Helper functions for creating visualizations
"""
import array
import matplotlib.pyplot as plt
import numpy as np
import matplotlib


def visualize_scatter(counts, codes, data, codebook, num_clusters, xlabel="", ylabel="", title=""):
    """
    Generates a 2-d scatter plot visualization of two feature data for 

    :param counts: dictionary of counts for the number of observations pairs for 
                    each cluster
    :param codes:  list of codes for each observation row in the order returned by the original query
    :param data: list of observations returned from query in their original order
    :param codebook: the coordinates of the centroids
    :param num_clusters: number of specified clusters up to 8
    :param xlabel: a label for the x axis (Default: None)
    :param ylabel: a label for the y axis (Default: None)
    """
    if num_clusters > 8:
        print "Visualize scatter only supports up to 8 clusters"
        return

    num_features = 2
    list_arrays = list()
    list_arr_idx = array.array("I", [0, 0, 0])

    for idx in range(num_clusters):
        list_arrays.append(np.zeros((counts[idx], num_features)))


    for i, j in zip(codes, data):

        list_arrays[i][list_arr_idx[i]][0] = j[0]
        list_arrays[i][list_arr_idx[i]][1] = j[1]
        list_arr_idx[i] += 1

    #plot the clusters first as relatively larger circles
    plt.scatter(codebook[:,0], codebook[:,1], color='orange', s=260)
   
    colors = ['red', 'blue', 'green', 'purple', 'cyan', 'black', 'brown', 'grey']
   
    for idx in range(num_clusters):
        plt.scatter(list_arrays[idx][:,0], list_arrays[idx][:,1], c=colors[idx]) 
    
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.show()



