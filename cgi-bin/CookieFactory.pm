package CookieFactory;

use CGI;
use Session;

sub new
{
  my $class = shift;
  my $self = {};
  
  bless $self;
  
  $self->{_url} = shift;
  
  return $self;
}

# Builds a cookie string from the passed values
sub buildCookie
{
	my $self = shift;
	my $name = shift;
	my $value = shift;
	my $expires = shift;		# e.g. +2d == 48 hours
	my $domain = shift;

	my $q = CGI->new();

	if ( ! $expires ) { $expires = "+1y"; }

	#my $domain = $self->{_url};
	#my $domain = '.' . $self->{_url};
	#my $domain = "flatpebble.nceas.ucsb.edu";
	#my $domain = "cx644539-b.santab1.ca.home.com";

	my $cookie = $q->cookie(-name    => $name,
				-value   => $value,
				-expires => $expires,
				-domain  => $domain,
				-path    => "/",
				-secure  => 0
				);
	return $cookie;
}

# Wrapper function to build the session_id cookie
sub buildSessionId
{
	my $self = shift;
	my $value = shift;
	my $cookie;

	$cookie = $self->buildCookie ( "session_id", $value, "+2d" );

	return $cookie;
}

sub getUniqueID
{
  my $self = shift;
  require Digest::MD5;
  my $md5 = Digest::MD5->new();
  my $remote = $ENV{REMOTE_ADDR} . $ENV{REMOTE_PORT};
  # Concatenates args: epoch, this interpreter PID, $remote (above)
  # returned as base 64 encoded string
  my $id = $md5->md5_base64(time, $$, $remote);
  # replace + with -, / with _, and = with .
  $id =~ tr|+/=|-_.|;
  return $id;
}

1;
