# 
# The Paleobiology Database
# 
#   ExternalIdent.pm
# 
# This module implements the recognition and generation of external
# identifiers for paleobiology database objects.


package ExternalIdent;

use strict;

use Carp qw(croak);

use base 'Exporter';

our (@EXPORT_OK) = qw(VALID_IDENTIFIER extract_identifier generate_identifier extract_num %IDP %IDRE);


# List the identifier prefixes:

our %IDP = ( URN => '(?:(?:urn:lsid:)?paleobiodb.org:|pbdb:)',
	     TID => 'txn|var',
	     TXN => 'txn',
	     VAR => 'var',
	     OPN => 'opn',
	     REF => 'ref',
	     OCC => 'occ',
	     REI => 'rei',
	     OID => 'occ|rei',
	     SPM => 'spm',
	     MEA => 'mea',
	     ELT => 'elt',
	     COL => 'col',
	     INT => 'int',
	     TSC => 'tsc',
	     CLU => 'clu',
	     PHP => 'php',
	     PRS => 'prs' );

our %IDRE;
our %IDVALID;

# VALID_IDENT ( type )
# 
# Return a closure which will validate identifiers of the given type.
# Acceptable values are any positive integer <n>, optionally preceded by the
# type, optionally preceded by the common URN prefix.  So, for example, each
# of the following are equivalent for specifying taxon number 23021:
# 
# 23021, txn23021, urn:paleobiodb.org:txn23021

sub VALID_IDENTIFIER {
    
    my ($type) = @_;
    
    croak "VALID_IDENTIFIER requires a parameter indicating the identifier type" unless $type;
    croak "No validator is defined for '$type'" unless ref $IDVALID{$type} eq 'CODE';
    
    return $IDVALID{$type};
    # return sub { return valid_identifier(shift, shift, $type) };
}


# Construct regular expressions to validate the various identifier types, and
# then define a validator for each type.


my $key_expr = '';

foreach my $key ( keys %IDP )
{
    next if $key eq 'URN';
    $IDRE{$key} = qr{ ^ (?: (?: $IDP{URN} )? ( $IDP{$key} ) [:] )? ( [0]+ | [1-9][0-9]* | ERROR ) $ }xsi;
    $IDVALID{$key} = sub { return valid_identifier(shift, shift, $key) };
    $key_expr .= '|' if $key_expr;
    $key_expr .= $IDP{$key};
}

$IDRE{UNKTXN} = qr{ ^ (?: (?: $IDP{URN} )? txn [:] )? ( [UN] [A-Z] \d* ) $ }xsi;

$IDRE{ANY} = qr{ ^ (?: (?: $IDP{URN} )? ( $key_expr ) [:] )? ( [0] | [1-9][0-9]* | ERROR ) $ }xsi;
$IDVALID{ANY} = sub { return valid_identifier(shift, shift, 'ANY') };

$IDRE{LOOSE} = qr{ ^  (?: (?: $IDP{URN} )? ( \w+ ) [:] )? ( [0]+ | [1-9][0-9]* | ERROR ) $ }xsi;

# valid_ident ( value, context, type )
# 
# Validator subroutine for paleobiodb.org identifiers.

sub valid_identifier {

    my ($value, $context, $type) = @_;
    
    # If the value matches the regular expression corresponding to the
    # specified type, then return the integer identifier extracted from it.
    # The rest of the value can be safely ignored.
    
    if ( $value =~ $IDRE{$type} )
    {
	my $idtype = $1;
	my $idnum = ($2 eq 'ERROR') ? -1 : $2;
	
	return { value => PBDB::ExtIdent->new($idtype, $idnum) };
    }
    
    # Check for "unknown taxon" identifiers.

    elsif ( ($type eq 'TID' || $type eq 'TXN') && $value =~ $IDRE{UNKTXN} )
    {
	my $idtype = 'txn';
	my $idnum = $1;
	$idnum =~ s/^U/N/;

	return { value => PBDB::ExtIdent->new($idtype, $idnum) };
    }
    
    # Otherwise, attempt to provide a useful error message.  If the value
    # contains a comma, note that we only accept a single identifier.  Any
    # parameter rule that requires more than one should use "list => ','"
    # which will split out the values before this function is ever called.
    
    my $msg; my $insert = '';
    
    $insert = ", optionally prefixed by 'paleobiodb.org:'" if $value =~ /paleobiodb|pbdb/;
    
    if ( $value =~ /,/ )
    {
	$msg = "the value of {param} must be a single identifier";
    }
    
    elsif ( $value =~ $IDRE{LOOSE} )
    {
	$msg = "the value of {param} must be an identifier of type $IDP{$type}$insert (type '$1' is not allowed with this operation)";
    }
    
    elsif ( $type eq 'ANY' )
    {
	$msg = "each value of {param} must be either a valid identifier of the form 'type:nnnn' " .
	    "where nnnn is an integer$insert, or a nonnegative integer (was {value})";
    }
    
    elsif ( $IDP{$type} =~ qr{(\w+)[|](\w+)} )
    {
	$msg = "each value of {param} must be either a valid identifier of the form '$1:nnnn' or " .
	   "'$2:nnnn' where nnnn is an integer$insert, or a nonnegative integer (was {value})";
    }
    
    else
    {
	$msg = "each value of {param} must be either a valid identifier or a nonnegative integer (was {value})";
    }
    
    return { error => $msg };
}


# extract_identifier ( type, value )
# 
# If the parameter $value contains a valid identifier of the specified type, extract and return
# the numeric id.  Otherwise, return undefined.

sub extract_identifier {

    my ($type, $value) = @_;
    
    if ( $value =~ $IDRE{$type} )
    {
	my $type = $1;
	my $num = ($2 eq 'ERROR') ? -1 : $2;
	
	return PBDB::ExtIdent->new($type, $num);
    }
    
    else
    {
	return;
    }
}


sub extract_num {
    
    my ($type, $value) = @_;
    
    if ( $value =~ $IDRE{$type} )
    {
	return ($2 eq 'ERROR') ? -1 : $2;
    }
    
    else
    {
	return;
    }
}


# generate_ident ( type, id )
# 
# Generate a valid identifier of the specified type.  If the id is 0, then
# return the empty string.  If it is a positive integer, return the proper
# identifier string.  Otherwise, return an indication of a bad value.

sub generate_identifier {
    
    my ($type, $value) = @_;
    
    if ( ref $value eq 'ARRAY' )
    {
	map { $_ = defined $_ && $_ > 0 ? "$IDP{$type}:$_" : "$IDP{$type}:ERROR" } @$value;
    }
    
    elsif ( defined $value && $value > 0 )
    {
	return "$IDP{$type}:$value";
    }
    
    elsif ( defined $value && $value eq '0' )
    {
	return '';
    }
    
    elsif ( defined $value && $value =~ qr{ ^ [UN][A-Z] \d* $ }xs )
    {
	return "$IDP{$type}:$value";
    }
    
    elsif ( defined $value )
    {
	return "$IDP{$type}:ERROR";
    }
    
    else
    {
	return;
    }
}


package PBDB::ExtIdent;
use overload '""' => \&stringify, fallback => 1;

use Carp qw(croak);

sub new {
    
    my ($class, $type, $num) = @_;
    
    croak "bad call to new: missing arguments\n" unless defined $num && $num >= 0;
    
    $type = lc ($type // 'unk');
    
    my $new = { type => $type, num => $num };
    
    $new->{taxon_no} = $num if $type eq 'var' || $type eq 'txn';
    
    return bless $new, $class;
}

sub stringify {
    
    my ($id) = @_;
    
    return $id->{num};
}


sub regenerate {
    
    my ($id) = @_;
    
    if ( $id->{type} eq 'unk' )
    {
	return $id->{num};
    }
    
    else
    {
	return "$id->{type}:$id->{num}";
    }
}

1;
