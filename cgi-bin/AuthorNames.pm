package AuthorNames;

# Token type constants
my $INITIAL = 1;
my $LNAME = 2;
my $SUFFIX = 3;
my $AND = 4;
my $COMMA = 5;
my $UNKNOWN_TYPE = 6;
my $ETAL = 7;

my $DEBUG = 0;
  
sub new
{
	my ($class, $data) = @_;
	my $self = {};
	
	bless $self, $class;
	
  if(UNIVERSAL::isa($data, 'DataRow'))
  {
	$self->dbg("Initializing via a DataRow object<br>");
    $self->setAuthor1Init($data->getValue('author1init'));
    $self->setAuthor1Last($data->getValue('author1last'));
    $self->setAuthor2Init($data->getValue('author2init'));
    $self->setAuthor2Last($data->getValue('author2last'));
    $self->setOtherAuthors($data->getValue('otherauthors'));
  }
  # Deal with a hash, too
  elsif(UNIVERSAL::isa($data,"HASH")){
	$self->dbg("Initializing via a HASH<br>");
	$self->setAuthor1Init($data->{'author1init'});
    $self->setAuthor1Last($data->{'author1last'});
    $self->setAuthor2Init($data->{'author2init'});
    $self->setAuthor2Last($data->{'author2last'});
    $self->setOtherAuthors($data->{'otherauthors'});

  }

  # Assume it's a string
  else{
	  $self->dbg("Initializing via a string<br>");
	  #$data = $self->toString();
	  #$self->setAuthorsString($data) if $data;
	  $self->setAuthorsString($data);
  }
  
	return $self;
}

sub setAuthorsString
{
  my ($self, $auString) = @_;
  
  $self->{au_string} = $auString;
  
  $self->parseAuthorsString($self->{au_string});
}

# Parses the supplied string and returns an ordered list of authors
# The reason this method is such a bitch is that the input is very
# poorly constrained.  All of the following input forms are acceptable:
#
# (Implementation of this function will take exactly one mythical man month)
#
#  * K. T. Frog
#  * K. T. Frog, et al.
#  * K. T. Frog, et alia
#  * K. T. Frog and R. M. Nixon
#  * K.T. Frog and R.M. Nixon
#  * K. T. Frog & R. M. Nixon
#  * K. T. Frog, R. M. Nixon and C. Kangaroo
#  * K. T. Frog, R. M. Nixon & C. Kangaroo
#  * K. T. Frog, R. M. Nixon, C. Kangaroo
#  * K. T. Frog and R. M. Nixon and C. Kangaroo
#  * Frog K. T., Nixon R. M.
#  * Frog K. T., and Nixon R. M.
#  * Frog K. T., & Nixon R. M.
#  * Frog K.T., & Nixon R.M.
# Dots and spaces may be omitted from initials. The trailing dot following
# 'et al.' may be omitted.  The leading ', ' before 'et al.' may be omitted.
# Initials may be omitted altogether for any author
#
# Any name may have a suffix consisting of any one of the
# following:
#  * Jr.
#  * Sr.
#  * II
#  * III
# This suffix, if it appears, may or may not be preceded by ', '
#
# '.' may be omitted in any of the above

sub parseAuthorsString
{
  my ($self, $inStr) = @_;
  
  # Valid forms to return are:
  #   Jones and Smith
  #   Jones et al.
  
  # Bust the string at the spaces
  my $inString = $inStr;
  # Get rid of all the dots (we'll put them in the appropriate places later)
  $inString =~ s/\.//g;
  # Combine 'et al' into a single token (we fix this below)
  $inString =~ s/\bet\.?\s+al\.?(ia)?\.?\b/et_al/;
  # Get rid of commas before suffixes (we'll put these back later)
  $inString =~ s/,\s*(jr\.?|sr\.?|i{2,3}|iv)/ $1/gi;
  # Surround commas with spaces (this turns commas into tokens because we split on spaces)
  $inString =~ s/,/ , /g;
  
  my @rawTokens = split(/\s+/, $inString);
  my @tokens;
  my @tokenTypes;
  my $tokenType;
  my $prevTokenType;
  my $numRawTokens = @rawTokens;
  
  my @curTokenList;
  
  my $au1Init;
  my $au1Last;
  my $au2Init;
  my $au2Last;
  my $otherAuthors;
  my $stage = 1;
  for(my $i = 0;$i < $numRawTokens;$i++){
    my $token = $rawTokens[$i];
    
    # Fix 'et_al.'
    $token =~ s/et_al/et al/;
    
    my $tokenType = $self->getTokenType($token);
    
    $self->dbg("$stage:$token:$tokenType\n");
    
    ## OK, the quick version
    # If we're in the first name
    if($stage == 1)
    {
      # If this is an 'et al', we've completed the first author's name.  Set the stage to 2
      if($tokenType == $ETAL)
      { 
        $stage = 2;
      }
      # Otherwise, if this is a suffix, append it to the last name
      elsif($tokenType == $SUFFIX)
      {
        $token =~ s/(.+)/\u$1/;
        $token =~ s/R\Z/r./i;
        $au1Last .= ", $token";
      }
      # Otherwise, if this is an initial
      elsif($tokenType == $INITIAL)
      {
        # If the last character was a comma, or an 'and', we have completed the first author's name
        # Set the stage to 2
        if($prevTokenType == $COMMA || $prevTokenType == $AND)
        {
          $prevTokenType = $tokenType;
          $stage = 2;
          $au2Init .= " $token";
          next;
        }
        # Otherwise, append it to the first author initials string
        else
        {
          $au1Init .= " $token";
        }
      }
      # Otherwise, if this is a name
      elsif($tokenType == $LNAME)
      {
        # If the last character was a comma, or an 'and' we have completed the first author's name
        # Set the stage to 2
        if($prevTokenType == $COMMA || $prevTokenType == $AND)
        {
          $prevTokenType = $tokenType;
          $stage = 2;
          $au2Last .= " $token";
		  $self->dbg("<br>TokenType=LNAME, Stage=1, au2Last: $au2Last<br>");
          next;
        }
        # Otherwise, append it to the first author name string
        else
        {
          $au1Last .= " $token";
        }
      }
      # If this is a comma, do nothing
    }
    # If we're in the second name
    if($stage == 2)
    {
      # If this is an 'et al'
      if($tokenType == $ETAL)
      {
        # If the second author's name is not null, we've completed the second author's name.  Set the stage to 3
        if($au2Last)
        {
          $stage = 3;
        }
        # Otherwise, set the second author's name to 'et al.', and call it a day.
        else
        {
          $au2Last = $token;
		  $self->dbg("<br>TokenType=ETAL, au2Last: $au2Last<br>");
          last;
        }
      }
      # Otherwise, if this is a suffix, append it to the second name
      elsif($tokenType == $SUFFIX)
      {
        $token =~ s/(.+)/\u$1/;
        $token =~ s/R\Z/r./i;
        $au2Last .= ", $token";
	    $self->dbg("<br>TokenType=SUFFIX, au2Last: $au2Last<br>");
      }
      # Otherwise, if this is an initial
      elsif($tokenType == $INITIAL)
      {
        # If the last character was a comma, or an 'and', we have completed the second author's name
        # Set the stage to 3
        if($prevTokenType == $COMMA || $prevTokenType == $AND)
        {
          $stage = 3;
        }
        # Otherwise, append it to the second author initials string
        else
        {
          $au2Init .= " $token";
        }
      }
      # Otherwise, if this is a name
      elsif($tokenType == $LNAME)
      {
        # If the last character was a comma, or an 'and' we have completed the second author's name
        # Set the stage to 3
        if($prevTokenType == $COMMA || $prevTokenType == $AND)
        {
          $stage = 3;
        }
        # Otherwise, append it to the second author name string
        else
        {
          $au2Last .= " $token";
	      $self->dbg("<br>TokenType=LNAME, Stage=2, au2Last: $au2Last<br>");
        }
      }
      # Otherwise, if this is a comma, do nothing
    }
    # If we're in the other authors
    if($stage == 3)
    {
      # Append whatever to the other authors string
      $otherAuthors .= " $token";
    }
    ## Boy, that was fun!
    
    $prevTokenType = $tokenType;
  }
  
  $self->setAuthor1Init($self->trimClean($au1Init));
  $self->setAuthor1Last($self->trimClean($au1Last));
  $self->setAuthor2Init($self->trimClean($au2Init));
  $self->setAuthor2Last($self->trimClean($au2Last));
  $self->setOtherAuthors($self->trimClean($otherAuthors));
  
  $self->dbg("Orig   : $inStr\n");
  $self->dbg("Fixd   : $inString\n");
  $self->dbg("Au1Init: " . $self->getAuthor1Init() . "\n");
  $self->dbg("Au1Last: " . $self->getAuthor1Last() . "\n");
  $self->dbg("Au2Init: " . $self->getAuthor2Init() . "\n");
  $self->dbg("Au2Last: " . $self->getAuthor2Last() . "\n");
  $self->dbg("Others : " . $self->getOtherAuthors() . "\n");
  $self->dbg("Tokens : " . join(':', @rawTokens) . "\n");
  $self->dbg("\n");
}

sub trimClean
{
  my ($self, $str) = @_;
  
  $str =~ s/\A\s+//;
  $str =~ s/\s+\Z//;
  $str =~ s/\s+,/,/g;
  
  return $str;
}

sub getTokenType
{
  my ($self, $token) = @_;
  
  my $tokenType;
  
  if($self->isInitial($token))
  {
    $tokenType = $INITIAL;
  }
  elsif($self->isAnd($token))
  {
    $tokenType = $AND;
  }
  elsif($self->isEtAl($token))
  {
    $tokenType = $ETAL;
  }
  elsif($self->isSuffix($token))
  {
    $tokenType = $SUFFIX;
  }
  elsif($self->isLName($token))
  {
    $tokenType = $LNAME;
  }
  elsif($self->isComma($token))
  {
    $tokenType = $COMMA;
  }
  else
  {
    $tokenType = $UNKNOWN_TYPE;
    $self->dbg("Unknown token type: $token\n");
  }
  
  return $tokenType;
}

sub isInitial
{
  my ($self, $token) = @_;
  
  # True if token is exactly three upper case characters JA 27.3.02
  # Possibly followed by a period and/or a space for each one 12/03/02 PM
  return 1 if $token ne 'III' && $token =~ /^[A-Z]\.{0,1}\s{0,1}[A-Z]\.{0,1}\s{0,1}[A-Z]\.{0,1}\s{0,1}$/ && uc($token) eq $token;

  # ... or two upper case characters JA 14.3.02
  # Possibly followed by a period and/or a space for each one 12/03/02 PM
  return 1 if $token ne 'II' && $token ne 'IV' && $token =~ /^[A-Z]\.{0,1}\s{0,1}[A-Z]\.{0,1}\s{0,1}$/ && uc($token) eq $token;

  # True if token is a single upper case character possibly followed
  # by a dot, followed by a dash, followed by another character and dot.
  return 1 if $token =~ /^[A-Z]\.{0,1}-[A-Za-z]\.{0,1}$/;
  # 01/07/03:  Foote wants to enter lowercase initials, such as 'J.-y.'
  #return 1 if $token =~ /^[A-Z]\.{0,1}-[A-Za-z]\.{0,1}$/ && uc($token) eq $token;
  
  # True if token is a single upper case character possibly followed
  # by a dot
  return 1 if $token =~ /^[A-Z]\.{0,1}$/ && uc($token) eq $token;

  return 0;
}

sub isLName
{
  my ($self, $token) = @_;
  
  # True if token contains at least two of any character, and is a mixed
  # case string
  return 1 if $token =~ /\A.{2,}\Z/ && $token =~ /[A-Z]/ && $token =~ /[a-z]/;
  
  return 0;
}

sub isSuffix
{
  my ($self, $token) = @_;
  
  # True if token is any of 'Jr', 'Sr', 'II' or 'III', 'IV' possibly
  # "iv" must fill entire token JA 1.10.02
  return 1 if $token =~ /^(jr\.?|sr\.?|i{2,3}|iv)$/i;
  
  return 0;
}

sub isEtAl
{
  my ($self, $token) = @_;
  
  # True if token is 'et al'
  return 1 if $token =~ /\Aet al\Z/;
  
  return 0;
}

sub isAnd
{
  my ($self, $token) = @_;
  
  # True if token is either of 'and' or '&'
  return 1 if $token =~ /\A(and|\&)\Z/;
  
  return 0;
}

sub isComma
{
  my ($self, $token) = @_;
  
  # True if token is a comma
  return 1 if $token eq ',';
}

sub getAuthor1Init
{
  my $self = shift;
  
  return $self->{au1Init};
}

sub setAuthor1Init
{
  my ($self, $auInit) = @_;
  
  # Get rid of all periods
  $auInit =~ s/\.//g;
  # Put them back in after every letter.
  $auInit =~ s/([A-Za-z])/$1./g;
  
  $self->{au1Init} = $auInit unless $auInit eq '';
}

sub getAuthor1Last
{
  my $self = shift;
  
  return $self->{au1Last};
}

sub setAuthor1Last
{
  my ($self, $auLName) = @_;

  $self->{au1Last} = $auLName unless $auLName eq '';
}

sub getAuthor2Last
{
  my $self = shift;
  
  return $self->{au2Last};
}

sub setAuthor2Last
{
  my ($self, $auLName) = @_;
  
  $auLName =~ s/et al\.?/et al./;
  
  $self->{au2Last} = $auLName unless $auLName eq '';
}

sub getAuthor2Init
{
  my $self = shift;
  
  return $self->{au2Init};
}

sub setAuthor2Init
{
  my ($self, $auInit) = @_;
  
  # Get rid of all periods
  $auInit =~ s/\.//g;
  # Put them back in after every letter.
  $auInit =~ s/([A-Za-z])/$1./g;
  
  $self->{au2Init} = $auInit unless $auInit eq '';
}

sub getOtherAuthors
{
  my $self = shift;
  
  return $self->{auOther};
}

sub setOtherAuthors
{
  my ($self, $auOther) = @_;
  
  $auOther =~ s/et al\.?/et al./;
  
  # Get rid of all periods
  $auOther =~ s/\.//g;
  # Put them back in. (?)
  $auOther =~ s/\b(\w)\b/$1./g;
  
  $self->{auOther} = $auOther unless $auOther eq '';
}

sub getAuthorsString
{
  my $self = shift;
  
  my $au1init = $self->getAuthor1Init();
  my $au1last = $self->getAuthor1Last();
  my $au2init = $self->getAuthor2Init();
  my $au2last = $self->getAuthor2Last();
  my $otherAuthors = $self->getOtherAuthors();
  
  my $retVal = $au1init;
  $retVal .= ' ' if $au1init && $au1last;
  $retVal .= $au1last;
  if($au2last =~ /et al/)
  {
    $retVal .= " $au2last";
    return $retVal;
  }
  if(($au1init || $au1last) && ($au2init || $au2last) && $otherAuthors)
  {
    $retVal .= ', ';
  }
  elsif(($au1init || $au1last) && ($au2init || $au2last))
  {
    $retVal .= ' and ';
  }
  $retVal .= $au2init;
  $retVal .= ' ' if $au2init && $au2last;
  $retVal .= $au2last;
  if($otherAuthors =~ /et al/)
  {
    $retVal .= " $otherAuthors";
    return $retVal;
  }
  if($otherAuthors)
  {
    my $otherAu = AuthorNames->new($otherAuthors);
    if($otherAu->getAuthor2Last())
    {
	  $self->dbg("Created new AuthorNames for OtherAuthors: $otherAuthors<br>");
      $retVal .= ', ';
    }
    else
    {
      $retVal .= ', and ';
    }
    $retVal .= $otherAu->toString();
  }
  return $retVal;
}

sub toString
{
  my $self = shift;
  
  return $self->getAuthorsString();
}

sub addNamesToRow
{
  my ($self, $drow) = @_;
  
  $drow->addColumn('author1last', 0, 0);
  $drow->setValue('author1last', $self->getAuthor1Last());
}

sub getHash
{
  my $self = shift;
  
  my %RETVAL;
  
  $RETVAL{'au1init'} = $self->getAuthor1Init();
  $RETVAL{'au1last'} = $self->getAuthor1Last();
  $RETVAL{'au2init'} = $self->getAuthor2Init();
  $RETVAL{'au2last'} = $self->getAuthor2Last();
  $RETVAL{'otherAuthors'} = $self->getOtherAuthors();
  
  return $RETVAL;
}

sub dbg
{
  my ($self, $str) = @_;
  
  print "<font color=\"green\">$str</font>" if $DEBUG;
}

1;
