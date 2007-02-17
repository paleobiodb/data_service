#!/usr/bin/perl

use Class::Date qw(date localdate gmdate now);

use strict;

open RLI, "</var/log/httpd/request_log"
    or die "Could not open request_log";
open RLO, ">>/var/log/httpd/request_log_resolved"
    or die "Could not open request_log_resolved";


my $last_line_resolved = `tail -1 /var/log/httpd/request_log_resolved`;
my ($ip,$date,@rest) = split(/\t/,$last_line_resolved);
my $time = 0;
my $lastDate;
if ($date) {
#    print "Last date resolved: $date\n";
    $lastDate = new Class::Date $date;
} else {
    $lastDate = new Class::Date "2000-01-01";
}


my $skipCount = 0;
my $resolveCount = 0;
while (my $line = <RLI>) {
    my ($ip,$date,@rest) = split(/\t/,$line);

    if ($ip =~ /^(\d{1,3}\.){3}\d{1,3}$/) { 
        my $lineDate = new Class::Date $date;

        if ($lineDate >= $lastDate) {
            $resolveCount++;
            my $text = `host $ip`;
            chomp($text);
            my $host;
            if ($text =~ /domain name pointer (.*?)$/) {
                $host = $1;
                $host =~ s/\.$//;
            } else {
                $host = $ip;
    #            print "Can't lookup $ip: $text\n";
            }
            print RLO "$host\t$date\t".join("\t",@rest);
        } else {
            $skipCount++;
        }
    }
}

#print "Skipped $skipCount and resolved $resolveCount\n";
