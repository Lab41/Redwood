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
"""


import re

from redwood.filters import plugins

class Aggregator():
    
    def __init_(self, cnx):
        self.cnx = cnx


    #should come in as a:x, b:y, c:z, etc, where x+y+z = 100, and a-c are filter ids
    #standard aggregate is equally weighted
    def aggregate(self, dist_str=None):
        
        weights = list()

        if dist_str is None:
            p = re.compile('\d+:\d+')
            found = p.findall(dist_str)
            for f in found:
                weights.append(f.split(':'))
        else:
            i = 0
            even_split = 100 / len(plugins)
            for p in plugins:
                weights.append((i, even_split))

      
        

        query = """
        UPDATE unique_file  
        LEFT JOIN fp_scores ON fp_scores.id = unique_file.id
        LEFT JOIN lu_scores ON lu_scores.id = unique_file.id
        SET unique_file.reputation = (.5 * fp_scores.score + .5 * lu_scores.score)
        """

        for w in weights:
            fltr = plugins.get(w[0])


