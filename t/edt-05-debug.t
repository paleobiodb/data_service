#
# EditTransaction project
# -----------------------
# 
# This file contains unit tests for the ETBasicTest class, a subclass of EditTransaction
# whose purpose is to implement these tests.
# 
# debug.t : Test the methods for controlling debugging and error output
# 

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 6;

use ETBasicTest;
use EditTester qw(connect_to_database ok_eval ok_exception ok_new_edt
		  capture_mode ok_captured_output ok_no_captured_output
		  clear_captured_output);


# Establish an EditTester instance.

$DB::single = 1;

my $T;

# Save STDERR so we can capture it.

my $ORIG_ERR;
my $ERRLOG;

open($ORIG_ERR, '>&', STDERR);


# Check that debug mode does what it is supposed to do, by capturing STDERR.

subtest 'debug mode' => sub {
    
    $T = EditTester->new({ class => 'ETBasicTest', debug_mode => 1 });
    
    clear_captured_output;
    
    capture_mode(1);
    
    ok( $T->debug_mode, "T debug mode set" );
    
    my $edt = $T->new_edt();
    
    ok( $edt->debug_mode, "edt debug mode set" );
    
    # $ERRLOG = '';
    # close(STDERR);
    # open(STDERR, '>', \$ERRLOG);
    
    $edt->debug_line('TEST DEBUG');
    $edt->error_line('TEST ERROR');
    
    # close(STDERR);
    # open(STDERR, '>&', $ORIG_ERR);
    
    # is( $ERRLOG, "TEST DEBUG\nTEST ERROR\n", "debug and error output captured" );
    
    ok_captured_output( "TEST DEBUG\nTEST ERROR\n", "debug and error output captured" );
    
    clear_captured_output;
    
    $T = EditTester->new({ class => 'ETBasicTest' });
    
    capture_mode(1);
    
    ok( ! $T->debug_mode, "T debug mode cleared" );
    
    my $edt = $T->new_edt();
    
    ok( ! $edt->debug_mode, "edt debug mode cleared" );
    
    # $ERRLOG = '';
    # close(STDERR);
    # open(STDERR, '>', \$ERRLOG);
    
    $edt->debug_line('TEST DEBUG');
    $edt->error_line('TEST ERROR');
    
    # close(STDERR);
    # open(STDERR, '>&', $ORIG_ERR);
    
    # is( $ERRLOG, "TEST ERROR\n", "only error output captured" );
    
    ok_captured_output( "TEST ERROR\n", "only error output captured" );
    
    $T->debug_mode(1);
    is( $T->debug_mode, 1, "T debug mode set" );
    
    $T->debug_mode(0);
    is( $T->debug_mode, 0, "T debug mode cleared" );
};


# Check that errlog mode does what it is supposed to do, by capturing STDERR.

subtest 'errlog mode' => sub {
    
    $T = EditTester->new({ class => 'ETBasicTest', debug_mode => 1, errlog_mode => 0 });
    
    ok( ! $T->errlog_mode, "T errlog mode cleared" );
    ok( $T->debug_mode, "T debug mode set" );
    
    my $edt = ok_new_edt;
    
    ok( $edt->silent_mode, "edt silent mode set" );
    ok( $edt->debug_mode, "edt debug mode set" );
    
    # $ERRLOG = '';
    # close(STDERR);
    # open(STDERR, '>', \$ERRLOG);
    
    capture_mode(1);
    clear_captured_output;
    
    $edt->debug_line('TEST DEBUG');
    $edt->error_line('TEST ERROR');
    
    # close(STDERR);
    # open(STDERR, '>&', $ORIG_ERR);
    
    # is( $ERRLOG, "TEST DEBUG\nTEST ERROR\n", "debug and error output captured" );
    
    ok_captured_output( "TEST DEBUG\nTEST ERROR\n", "debug and error output captured" );
    
    $T = EditTester->new({ class => 'ETBasicTest', errlog_mode => 0 });
    
    ok( ! $T->errlog_mode, "T errlog mode cleared" );
    
    my $edt = ok_new_edt;
    
    ok( $edt->silent_mode, "edt silent mode set" );
    
    clear_captured_output;
    
    # $ERRLOG = '';
    # close(STDERR);
    # open(STDERR, '>', \$ERRLOG);
    
    $edt->debug_line('TEST DEBUG');
    $edt->error_line('TEST ERROR');
    
    # close(STDERR);
    # open(STDERR, '>&', $ORIG_ERR);
    
    # is( $ERRLOG, '', "neither error nor debug output captured" );
    
    ok_no_captured_output( "neither error nor debug output captured" );
};


# Check that we can set and clear the flags during a transaction.

subtest 'setting and clearing' => sub {
    
    my $T = EditTester->new('ETBasicTest');
    
    my $edt = $T->new_edt();
    
    ok( ! $edt->debug_mode, "edt starts with debug mode cleared" );
    
    $edt->debug_mode(1);
    
    clear_captured_output;
    
    capture_mode(1);
    
    # $ERRLOG = '';
    # close(STDERR);
    # open(STDERR, '>', \$ERRLOG);
    
    $edt->debug_line('TEST DEBUG');
    $edt->error_line('TEST ERROR');
    
    # close(STDERR);
    # open(STDERR, '>&', $ORIG_ERR);
    
    # is( $ERRLOG, "TEST DEBUG\nTEST ERROR\n", "debug and error output captured" );
    
    ok_captured_output( "TEST DEBUG\nTEST ERROR\n", "debug and error output captured" );
    
    clear_captured_output;
    
    $edt->debug_mode(0);
    
    # $ERRLOG = '';
    # close(STDERR);
    # open(STDERR, '>', \$ERRLOG);
    
    $edt->debug_line('TEST DEBUG');
    $edt->error_line('TEST ERROR');
    
    # close(STDERR);
    # open(STDERR, '>&', $ORIG_ERR);
    
    ok_captured_output( "TEST ERROR\n", "only error output captured" );
    
    clear_captured_output;
    
    # is( $ERRLOG, "TEST ERROR\n", "only error output captured" );
    
    $edt->silent_mode(1);
    
    # $ERRLOG = '';
    # close(STDERR);
    # open(STDERR, '>', \$ERRLOG);
    
    $edt->debug_line('TEST DEBUG');
    $edt->error_line('TEST ERROR');
    
    # close(STDERR);
    # open(STDERR, '>&', $ORIG_ERR);
    
    # is( $ERRLOG, "", "no output captured" );
    
    ok_no_captured_output( "no output captured" );
    
    $edt->debug_mode(1);
    
    # $ERRLOG = '';
    # close(STDERR);
    # open(STDERR, '>', \$ERRLOG);
    
    $edt->debug_line('TEST DEBUG');
    $edt->error_line('TEST ERROR');
    
    # close(STDERR);
    # open(STDERR, '>&', $ORIG_ERR);
    
    # is( $ERRLOG, "TEST DEBUG\nTEST ERROR\n", "error and debug output captured" );
    
    ok_captured_output( "TEST DEBUG\nTEST ERROR\n", "error and debug output captured" );
    
    clear_captured_output;
    
    $edt->debug_mode(0);
    $edt->silent_mode(0);
    
    # $ERRLOG = '';
    # close(STDERR);
    # open(STDERR, '>', \$ERRLOG);
    
    $edt->debug_line('TEST DEBUG');
    $edt->error_line('TEST ERROR');
    
    # close(STDERR);
    # open(STDERR, '>&', $ORIG_ERR);
    
    # is( $ERRLOG, "TEST ERROR\n", "only error output captured" );
    
    ok_captured_output( "TEST ERROR\n", "only error output captured" );
    
    clear_captured_output;
};


# Check that debug and errlog modes can be set from the command line, overriding calls in the
# code.

subtest 'command line' => sub {
    
    my $save = $ENV{DEBUG};
    $ENV{DEBUG} = 1;
    
    $T = EditTester->new({ class => 'ETBasicTest', debug_mode => 0, errlog_mode => 0 });
    
    ok( $T->debug_mode, "T debug mode is set" );
    ok( ! $T->errlog_mode, "T errlog mode is cleared" );
    
    my $edt = $T->new_edt();
    
    ok( $edt->debug_mode, "edt debug mode is set" );
    ok( $edt->silent_mode, "edt silent mode is set" );
    
    $ENV{DEBUG} = $save;
    
    my $save2 = $ENV{PRINTERR};
    $ENV{PRINTERR} = 1;
    
    $T = EditTester->new({ class => 'ETBasicTest', debug_mode => 0, errlog_mode => 0 });
    
    ok( ! $T->debug_mode, "T debug mode is cleared" );
    ok( $T->errlog_mode, "T errlog mode is set" );
    
    my $edt = $T->new_edt();
    
    ok( ! $edt->debug_mode, "edt debug mode is cleared" );
    ok( ! $edt->silent_mode, "edt silent mode is cleared" );
    
    $ENV{PRINTERR} = $save2;
    
    unshift @ARGV, 'debug';
    
    $T = EditTester->new({ class => 'ETBasicTest', debug_mode => 0, errlog_mode => 0 });
    
    ok( $T->debug_mode, "T debug mode is set" );
    ok( ! $T->errlog_mode, "T errlog mode is cleared" );
    
    shift @ARGV;
    
    unshift @ARGV, 'errlog';
    
    $T = EditTester->new({ class => 'ETBasicTest', debug_mode => 0, errlog_mode => 0 });
    
    ok( ! $T->debug_mode, "T debug mode is cleared" );
    ok( $T->errlog_mode, "T errlog mode is set" );
    
    shift @ARGV;
};


# Check that the debug and errlog flags can be altered by mode arguments.

subtest 'modes' => sub {
    
    $T = EditTester->new('ETBasicTest');
    
    ok( ! $T->debug_mode, "T debug mode not set" );
    ok( ! $T->errlog_mode, "T errlog mode not set" );
    
    my $edt = $T->new_edt('DEBUG_MODE');
    
    ok( $edt->debug_mode, "edt debug mode set" );
    ok( ! $edt->silent_mode, "edt silent mode cleared" );
    
    my $edt = $T->new_edt('NO_DEBUG_MODE', 'SILENT_MODE');
    
    ok( ! $edt->debug_mode, "edt debug mode cleared" );
    ok( $edt->silent_mode, "edt silent mode set" );
    
    my $edt = $T->new_edt('NO_SILENT_MODE');
    
    ok( ! $edt->debug_mode, "edt debug mode cleared" );
    ok( ! $edt->silent_mode, "edt silent mode cleared" );
};


# Check that an indirect argument (a request object) can receive debug and error output.

subtest 'indirect' => sub {
    
    capture_mode(0);
    
    my $request = TestIndirect->new($T->dbh);
    
    $T = EditTester->new({ class => 'ETBasicTest', request => $request });
    
    $T->new_edt('DEBUG_MODE', label =>  "new edt with request object" );
    
    if ( my $edt = $T->last_edt )
    {
	$edt->debug_line('TEST DEBUG');
	$edt->error_line('TEST ERROR');
	
	is( $request->errlog, "TEST DEBUG\nTEST ERROR\n", "error and debug output captured" );
    }
};


package TestIndirect;

our ($ERRLOCAL) = '';

sub new {
    
    return bless { dbh => $_[1] };
}


sub get_connection {
    
    my ($self) = @_;
    
    return $self->{dbh};
}


sub debug_line {
    
    my ($self, $line) = @_;
    
    $ERRLOCAL .= "$line\n";
}


sub error_line {
    
    my ($self, $line) = @_;
    
    $ERRLOCAL .= "$line\n";
}

sub clear {
    
    $ERRLOCAL = '';
}


sub errlog {
    
    return $ERRLOCAL;
}
