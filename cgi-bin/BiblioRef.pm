package BiblioRef;

use DataRow;
use AuthorNames;
use Globals;

sub new
{
  my ($class, $data) = @_;
  
  my $self = {};
  bless $self, $class;
  
  if(UNIVERSAL::isa($data, 'DataRow'))
  {
    $self->{_an} = AuthorNames->new($data);
    
    $self->{_reference_no} = $data->getValue('reference_no');
    $self->{_project_name} = $data->getValue('project_name');
	$self->{_project_name} =~ s/,/,&nbsp;/g;
    $self->{_project_ref_no} = $data->getValue('project_ref_no');

    $self->{_authorizer} = $data->getValue('authorizer');
    $self->{_enterer} = $data->getValue('enterer');
    $self->{_modifier} = $data->getValue('modifier');
    
    $self->{_author1init} = $data->getValue('author1init');
    $self->{_author1last} = $data->getValue('author1last');
    $self->{_author2init} = $data->getValue('author2init');
    $self->{_author2last} = $data->getValue('author2last');
    $self->{_otherauthors} = $data->getValue('otherauthors');
    
    $self->{_pubyr} = $data->getValue('pubyr');
    $self->{_reftitle} = $data->getValue('reftitle');
    $self->{_pubtitle} = $data->getValue('pubtitle');
    $self->{_pubvol} = $data->getValue('pubvol');
    $self->{_pubno} = $data->getValue('pubno');
    $self->{_firstpage} = $data->getValue('firstpage');
    $self->{_lastpage} = $data->getValue('lastpage');
    $self->{_created} = $data->getValue('created');
    $self->{_publication_type} = $data->getValue('publication_type');
  }
  
  return $self;
}

# Make a table row out of the data row
sub toString
{
	my $self = shift;
	my $selectable = shift;
	my $row = shift;
	my $rowcount = shift;

	my $an = $self->{_an};
	my $retVal = "";
	my $bgcolor = "";

	# Choose the row background color
	if ( $row % 2 != 0 && $rowcount > 1) {
		$retVal .= "<tr class='darkList'>\n";
	}
	else	{
		$retVal .= "<tr>\n";
	}

	if ( $selectable ) {
		$retVal .= "	<td width=\"5%\" valign=top><input type=\"radio\" name=\"reference_no\" value=\"" . $self->{_reference_no} . "\"></td>\n";
	} else {
		# Nothing there?  Make it small
		$retVal .= "	<td></td>\n";
	}

	$retVal .= "	<td width=\"5%\" valign=top><b>".$self->{_reference_no}."</b></td>\n";

	# Project name
	if ($self->{_project_name})	{
		$retVal .= "	<td valign=\"top\"><font color=\"red\">".$self->{_project_name};
		$retVal .= " ".$self->{_project_ref_no} if $self->{_project_ref_no};
		$retVal .= "</font></td>\n";
	} else {
		# Nothing there?  Make it small
		$retVal .= "	<td></td>\n";
	}

	$retVal .= "	<td>\n";

	$retVal .= $an->toString();

	$retVal .= "." if $retVal && $retVal !~ /\.\Z/;
	$retVal .= " ";

	$retVal .= $self->{_pubyr}.". " if $self->{_pubyr};

	$retVal .= $self->{_reftitle} if $self->{_reftitle};
	$retVal .= "." if $self->{_reftitle} && $self->{_reftitle} !~ /\.\Z/;
	$retVal .= " " if $self->{_reftitle};

	$retVal .= "<i>" . $self->{_pubtitle} . "</i>" if $self->{_pubtitle};
	$retVal .= " " if $self->{_pubtitle};

	$retVal .= "<b>" . $self->{_pubvol} . "</b>" if $self->{_pubvol};

	$retVal .= "<b>(" . $self->{_pubno} . ")</b>" if $self->{_pubno};

	$retVal .= ":" if $self->{_pubvol} && ( $self->{_firstpage} || $self->{_lastpage} );

	$retVal .= $self->{_firstpage} if $self->{_firstpage};
	$retVal .= "-" if $self->{_firstpage} && $self->{_lastpage};
	$retVal .= $self->{_lastpage};
	# also displays authorizer and enterer JA 23.2.02
	$retVal .= "<font size=\"small\"> [".$self->{_authorizer}."/".
			   $self->{_enterer};
	if($self->{_modifier}){
		$retVal .= "/".$self->{_modifier};
	}
	$retVal .= "]</font>\n";
	$retVal .= "</td>\n";
	$retVal .= "</tr>\n";

	# tack on the ref no at the end so it can be used by getCollsWithRef
	# $retVal .= " " . $self->{_reference_no};
	
	return $retVal;
}

sub get {
	my $self = shift;
	my $key = shift;
	return $self->{$key};
}

1;
