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

import os
import shutil
import math
from redwood.filters import filter_list
from redwood.helpers import core
import matplotlib.pylab as plt

class Report():  
    def __init__(self, cnx):
        self.report_dir = "reports"          
        self.cnx = cnx


    #collects survey reports from each filter and aggregates the results into one central report
    def run_filter_survey(self, source_name=None):
        if source_name == None:        
            return
        
        for f in filter_list:
            f.cnx = self.cnx
            path = f.run_survey(source_name)
            try:
                shutil.rmtree(self.report_dir + "/" + source_name + "/filters/" + f.name)
            except:
                pass
            shutil.move(path, self.report_dir + "/" + source_name + "/filters/" + f.name)
        
    def generate_report(self, source):
        report_dir = "reports/" + source[0]
        report_file = source[0] + "_report.html"
        html_file = os.path.join(report_dir, report_file)

        with open(html_file, 'w') as f:
            f.write("""
            <html>
            <head>
            <link href="../resources/css/style.css" rel="stylesheet" type="text/css">
            </head>
            <body>
            <h2>Report for {}</h2>""".format(source[0]))
            f.write("<h3 class=\"redwood-header\">Source Information</h3>")
            f.write("<dl>")
            f.write("<dt>Acquisition Date: {}</dt>".format(source[1]))
            f.write("<dt>Operating System: {}</dt>".format(source[2]))
            f.write("</dl>")
            
            score_counts = core.get_repuation_by_source(self.cnx, source[0])
            scores = list()            
            counts = list()            
            
            for s in score_counts:
                scores.append(s[0])
                counts.append(s[1])
            #plt.close()
            #plt.clf()
            #plt.pie(counts, autopct='%1.f%%', shadow=True)
            #plt.title('File Reputations')
            #plt.show()
            
            table_height = int(math.ceil(len(score_counts) / float(3)))
            print len(score_counts)
            print table_height
            
            f.write("<table border=\"1\" id=\"rounded-corner\">")
            f.write("<thead></thead>")
            f.write("""
            <thead>
                <tr>
                    <th scope=\"col\" class=\"rounded-head-left\">Score</th>
                    <th scope=\"col\">Count</th>
                    <th scope=\"col\">Score</th>
                    <th scope=\"col\">Count</th>
                    <th scope=\"col\">Score</th>
                    <th scope=\"col\" class=\"rounded-head-right\">Count</th>
                </tr>
            </thead>
            <tbody>""")
            for i in range(0, table_height):
                if table_height * 2 + i >= len(score_counts):
                    if i == table_height - 1:
                        print "Arrived 3"
                        f.write("""
            </tbody>
            <tfoot>
                <tr>
                    <td class=\"rounded-foot-left\">{}</td>
                    <td>{}</td>
                    <td>{}</td>
                    <td>{}</td>
                    <td></td>
                    <td class=\"rounded-foot-right\"></td>
                </tr>
            </tfoot>""".format(score_counts[i][0], score_counts[i][1], \
                score_counts[table_height + i][0], score_counts[table_height + i][1]))
                    else:
                        print "Arrived 2"
                        f.write("""
                        <tr>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                            <td></td>
                            <td></td>
                        </tr>""".format(score_counts[i][0], score_counts[i][1], \
                        score_counts[table_height + i][0], score_counts[table_height + i][1]))
                else:
                    print "Arrived!"
                    f.write("""
                        <tr>
                            <td>{}</td>
                            <td class=score-divider>{}</td>
                            <td>{}</td>
                            <td class=score-divider>{}</td>
                            <td>{}</td>
                            <td>{}</td>
                        </tr>""".format(score_counts[i][0], score_counts[i][1], \
                    score_counts[table_height + i][0], score_counts[table_height + i][1], \
                    score_counts[table_height * 2 + i][0], score_counts[table_height * 2 + i][1]))
            f.write("</table>") 
            filter_survey = ""           
            #for d in os.listdir(report_dir + "/filters"):
            #    if os.path.isdir(os.path.join(report_dir + "/filters", d)) and d[0] != '.':
            #        filter_survey = os.path.join("filters/" + d, "survey.html")
            #        f.write("""
            #        <iframe src=\"{}\" width=\"100%\" height=\"100%\"></iframe>
            #            """.format(filter_survey))
            #f.write()

            f.write("</body></html>")
            f.close()