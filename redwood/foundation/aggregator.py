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


#import re
#from redwood.filters import redwood_filter

class Aggregator():
    
    def __init__(self, cnx):
        self.cnx = cnx


    #should come in as a:x, b:y, c:z, etc, where x+y+z = 100, and a-c are filter ids
    #standard aggregate is equally weighted
    def aggregate(self, filter_list, dist_str=None):
        
        weights = list()
        filters = list()

        if not dist_str is None:
            if len(dist_str) > len(filter_list):
                print ("Error there are only " + str(len(filter_list)) + 
                " filters, you supplied weights for " + str(len(dist_str)))
                return
            try:
                for s in dist_str:
                    p = s.split(':')
                    filter_id = int(p[0])
                    if filter_id in filters:
                        print ("Error mutliple values entered for filter " + 
                        str(filter_id))
                        return
                    filters.append(filter_id)
                    percent = float(p[1])
                    if percent > 1:
                        percent = percent / float(100)
                    weights.append((filter_id, percent))
                total = 0
                for w in weights:
                    total += w[1]
                if total != 1.0:
                    print "The filter weights must total 1 or 100"
                    return
                return
            except:
                print "There was an error with your sytax, try again"
                return
        else:
            i = 0
            even_split = 1 / float(len(filter_list))
            for f in filter_list:
                weights.append((i, even_split))
                i += 1

        query = """
        UPDATE unique_file  
        LEFT JOIN fp_scores ON fp_scores.id = unique_file.id
        LEFT JOIN lu_scores ON lu_scores.id = unique_file.id
        SET unique_file.reputation = (.5 * fp_scores.score + .5 * lu_scores.score)
        """
        
        query = "UPDATE unique_file\n"

        for w in weights:
            fltr = filter_list[w[0]]
            print fltr.name + " Weight " + str(w[1])
            query += "LEFT JOIN " + fltr.score_table + " ON " + fltr.score_table + ".id = unique_file.id\n"
            
        query += "SET unique_file.reputation = ("
        
        for w in weights:
            fltr = filter_list[w[0]]
            query += str(w[1]) + " * " + fltr.score_table + ".score + "
            
        query = query[0:len(query)-3]
        query += ")"
        print query
        cursor = self.cnx.cursor()
        cursor.execute(query)
        self.cnx.commit()
        cursor.close()