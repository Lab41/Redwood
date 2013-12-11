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
        report_dir = "reports/" + source.source_name
        report_file = source.source_name + "_report.html"
        html_file = os.path.join(report_dir, report_file)

        score_counts = core.get_repuation_by_source(self.cnx, source[0])            
        table_height = int(math.ceil(len(score_counts) / float(3)))
        file_count = 0
        for s in score_counts:
            file_count += s[1]

        with open(html_file, 'w') as f:
            f.write("""
            <html>
            <head>
            <link href="../resources/css/style.css" rel="stylesheet" type="text/css">
            </head>
            <body>
            <div id="navigation">
                <image class="center" src="../resources/images/redwood_logo.png" height="25%"/>
                <dl class="list">""")
                    #<dt><a href="">Navigation item 1</a></dt>
                    #<dt><a href="">Navigation item 2</a></dt>
                    #<dt><a href="">Navigation item 3</a></dt>
                    #<dt><a href="">Navigation item 4</a></dt>
            f.write("""
                </dl>
            </div>
            <div id="top">
                <h2 class="redwood-title">Report for {}</h2>\n""".format(source.source_name))
            f.write("\t\t<h3 class=\"redwood-header\">Source Information</h3>\n")
            f.write("\t\t<dl>\n")
            f.write("\t\t\t<dt>Acquisition Date: {}</dt>\n".format(source.date_acquired))
            f.write("\t\t\t<dt>Operating System: {}</dt>\n".format(source.os_name))
            f.write("\t\t\t<dt>File Count: {}</dt>\n".format(file_count))
            f.write("\t\t</dl>\n\t\t</div>\n")            
            f.write("\t\t<div id=\"content\">\n")
            f.write("\t\t<table border=\"1\" id=\"redwood-table\">\n")
            f.write("\t\t\t<caption class=\"caption\">File Score Distribution</caption>\n")
            f.write("\t\t\t<thead></thead>\n")
            f.write("""
            <thead>
                <tr>
                    <th scope="col" class="rounded-head-left">Score</th>
                    <th scope="col" class="count-divider">Count</th>
                    <th scope="col" class="score-divider">Score</th>
                    <th scope="col" class="count-divider">Count</th>
                    <th scope="col" class="score-divider">Score</th>
                    <th scope="col" class="rounded-head-right">Count</th>
                </tr>
            </thead>
            <tbody>""")
            for i in range(0, table_height):
                if table_height * 2 + i >= len(score_counts):
                    if i == table_height - 1:
                        f.write("""
            </tbody>
            <tfoot>
                <tr>
                    <td class="rounded-foot-left">{}</td>
                    <td class="count-divider">{}</td>
                    <td class="score-divider">{}</td>
                    <td class="count-divider">{}</td>
                    <td class="score-divider"></td>
                    <td class="rounded-foot-right"></td>
                </tr>
            </tfoot>""".format(score_counts[i][0], score_counts[i][1], \
                score_counts[table_height + i][0], score_counts[table_height + i][1]))
                    else:
                        f.write("""
                        <tr>
                            <td class="score-divider">{}</td>
                            <td class="count-divider">{}</td>
                            <td class="score-divider">{}</td>
                            <td class="count-divider">{}</td>
                            <td class="score-divider"></td>
                            <td></td>
                        </tr>""".format(score_counts[i][0], score_counts[i][1], \
                        score_counts[table_height + i][0], score_counts[table_height + i][1]))
                else:
                    f.write("""
                        <tr>
                            <td class="score-divider">{}</td>
                            <td class=count-divider>{}</td>
                            <td class="score-divider">{}</td>
                            <td class=count-divider>{}</td>
                            <td class="score-divider">{}</td>
                            <td>{}</td>
                        </tr>""".format(score_counts[i][0], score_counts[i][1], \
                    score_counts[table_height + i][0], score_counts[table_height + i][1], \
                    score_counts[table_height * 2 + i][0], score_counts[table_height * 2 + i][1]))
            f.write("""
            </table>
            </div>
            """)
            filter_survey = ""
            #for d in os.listdir(report_dir + "/filters"):
            #    if os.path.isdir(os.path.join(report_dir + "/filters", d)) and d[0] != '.':
            #        filter_survey = os.path.join("filters/" + d, "survey.html")
            #        f.write("""
            #        <iframe src=\"{}\" width=\"100%\" height=\"100%\"></iframe>
            #            """.format(filter_survey))

            f.write("</body></html>")
            f.close()