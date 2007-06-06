package PAST;

# Generic PBDB includes
use Text::CSV_XS;
use PBDBUtil;
use Data::Dumper;
use GD;
use Debug qw(dbg);
use Constants qw($HTML_DIR);

# Various internal modules
use PAST::Util;
use PAST::DCA;

use strict;

my $PAST_HTML_DIR = "/public/past";
my $PAST_FILE_DIR = $HTML_DIR.$PAST_HTML_DIR;
my $DOWNLOAD_FILE_DIR = $HTML_DIR."/public/downloads";

sub queryForm {
    my ($dbt,$q,$hbo,$s) = @_;

    my $filename = getDownloadFileName($q,$s);
    print <<EOF;
<div align="center"><h2>PAST functions</h2></div>

<p class="medium">
These functions were adapted by &Oslash;yvind Hammer from his free, easy-to-use <a href="http://folk.uio.no/ohammer/past/index.html">PAST</a> (PAlaeontological STatistics) data analysis package, which is written for the Windows operating system.
</p>

<p class="medium">
EOF

    if (! -e $filename) {
        print "You must do a <a href=\"bridge.pl?action=displayDownloadForm\">download</a> before you can use these functions.\n";
    } else	{
        print "You may want to do another <a href=\"bridge.pl?action=displayDownloadForm\">download</a> before using these functions.\n";
    }

    print <<EOF;
It is strongly recommended that you exclude collections with less than a certain number (say, 20) of occurrences.
This option is in the "Include collections" panel section of the download form.
If you want to view collection attributes like country, stage, formation, lithology, or environment, make sure to check the relevant boxes in the "Collection fields" section.
</p>
EOF

    if (! -e $filename) {
        return;
    }

    print <<EOF;
<p class="medium">Please choose a function and submit:</p>
<form method="POST" action="bridge.pl"> 
<input type="hidden" name="action" value="PASTQuerySubmit">
<select name="pastmodule">
  <option value="displaymatrix">Display presence matrix</option>
  <option value="dca">Detrended Correspondence Analysis</option>
</select>
<p>
<input type="submit" name="submit" value="Analyze data">
</p>
</form>
EOF
}

sub querySubmit {
    my ($dbt,$q,$hbo,$s) = @_;

    my $filename = getDownloadFileName($q,$s);
    my @download_data = PAST::Util::parseTextFile($filename);
    if (!@download_data) {
        dbg("Error parsing download file");
        return;
    } 
    my @matrix_data = PAST::Util::binaryPresenceMatrix(@download_data);
    #print Dumper(\@matrix_data);
    my @row_header = @{$matrix_data[0]};
    my @col_header = @{$matrix_data[1]};
    my @matrix = @{$matrix_data[2]};
    my %extra_data = %{$matrix_data[3]};

    #my $outfile = "foo.csv";
    #TBD writeCSV($PAST_FILE_DIR."/".$outfile,\@matrix,\@row_header,\@col_header);
    #TBD @data = PAST::DCA::dca(\@matrix,\@row_header,\@col_header);

    if ($q->param('pastmodule') eq "dca") {
      # also pass in extra_data so the user can display extra collection info
      PAST::DCA::dca(\@matrix,\@row_header,\@col_header,\%extra_data);
    } else { if ($q->param('pastmodule') eq "displaymatrix") {
      print "<div align=\"center\"><h2>Presence matrix</h2></div>";
      print "<div class=\"tiny\">\n";
      print "<table border=0 cellspacing=0 class=\"PASTtable\">\n<tr><td></td>\n";
      for my $i ( 0..$#col_header )	{
        print "<td class=\"PASTheader\">$col_header[$i]</td> ";
      }
      print "\n</tr>\n";
      for my $i ( 0..$#row_header )	{
        print "<tr><td class=\"PASTheader\">$row_header[$i]</td> ";
        for my $j ( 0..$#col_header )	{
          print "<td class=\"PASTcell\">$matrix[$i][$j]</td> ";
        }
        print "</tr>\n";
      }
      print "</tr>\n</table>\n";
      print "</div>\n";
      #print "<br><br><a href=\"$PAST_HTML_DIR/$outfile\">Download results</a>";
    }}
}


sub getDownloadFileName {
    my ($q,$s) = @_;
    my $enterer= $s->get('enterer');

    # BEGINNING of input file parsing routine
    my $name = ($s->get("enterer")) ? $s->get("enterer") : $q->param("yourname");
    my $base_filename = PBDBUtil::getFilename($name);
    my $tabfile = $DOWNLOAD_FILE_DIR."/$base_filename-occs.tab";
    my $csvfile = $DOWNLOAD_FILE_DIR."/$base_filename-occs.csv";

    my $filename;
    if ((-e $tabfile && -e $csvfile && ((-M $tabfile) < (-M $csvfile))) ||
        (-e $tabfile && !-e $csvfile)){
        $filename = $tabfile;
        dbg("using tab $filename");
    } else {
        $filename = $csvfile;
        dbg("using csv $filename");
    }
    return $filename;   
}

1;
