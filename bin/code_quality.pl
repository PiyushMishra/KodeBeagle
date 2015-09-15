#!/usr/bin/perl

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

use strict;


open FILE, "plugins/idea/kodebeagleidea/target/findbugs/report.xml" or die "Couldn't open file: $!"; 

my $string = "";
my $count = -1;

# Findbugs
while (<FILE>) {
    $string .= $_;
    if(/<Errors errors=\"(\d+).*/) {
        $count = $1;
    }
}
if ($count > 0) {
    print "ERROR: Found findbug violations.!!\n";
    print $string;
    exit(1);
}
print "SUCCESS: No findbug violations\n";
close FILE;

# Checkstyle
open FILE, "plugins/idea/kodebeagleidea/target/checkstyle-result.xml" or die "Couldn't open file: $!"; 

my $string = "";
my $count = 0;

while (<FILE>) {

    if(/<file .*/) {
        $string .= $_;
    }

    if(/<error .*/) {
        $string .= $_;
        $count = $count + 1;
    }
}

if ($count > 0) {
    print "ERROR: Found checkstyle violations.!!\n";
    print $string;
    exit(1);
}

print "SUCCESS: No checkstyle violations\n";
close FILE;

# PMD
open FILE, "plugins/idea/kodebeagleidea/target/pmd.xml" or die "Couldn't open file: $!"; 

my $string = "";
my $count = 0;

while (<FILE>) {

    if(/<file .*/) {
        $string .= $_;
    }

    if(/<violation .*/) {
        $string .= $_;
        $count = $count + 1;
    }
}

if ($count > 4) { # allowing 4 violations
    print "ERROR: Your code adds PMD violations.!!\n";
    print $string;
    exit(1);
}

print "SUCCESS: No PMD violations\n";
close FILE;
