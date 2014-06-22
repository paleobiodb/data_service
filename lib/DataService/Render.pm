#
# Web::DataService::Render
# 
# This module is responsible for rendering documentation pages and output
# pages via templates.
# 
# Author: Michael McClennen

use strict;

package Web::DataService;

use Carp qw(carp croak);




# check_doc ( template )
# 
# Return the given template filename if the corresponding file path exists and
# is readable under the documentation template directory.  Throw an exception
# if the file exists but is not readable.  Return undefined (false) if the
# file is not there.

sub check_doc {

    my ($self, $template) = @_;
    
    return unless defined $template && $template ne '';
    
    my $template_file = $self->{doc_templates} . '/' . $template;
    
    if ( -e $template_file )
    {
	return $template if -r $template_file;
	croak "template $template_file: $!";
    }
    
    return;
}


# render_doc ( template, definitions, header, footer, vars )
# 
# Render the specified template, with the specified parameters.  If
# $definitions and/or $header are defined, process them before the main
# template.  If $footer is defined, process it after the main template.  If
# $vars is defined, it must be a hashref of variable definitions that will be
# passed to the template rendering engine.

sub render_doc {
    
    my ($ds, $main, $defs, $header, $footer, $vars) = @_;
    
    # Throw an exception if no templating module was selected.
    
    croak "you must select a templating module"
	unless defined $ds->{templating_plugin};
    
    my $templates = { defs => $defs, header => $header,
		      main => $main, footer => $footer };
    
    $ds->{templating_plugin}->render_template($ds, $ds->{doc_engine}, $vars, $templates);
}


# check_output ( template )
# 
# Return the given template filename if the corresponding file path exists and
# is readable under the output template directory.  Throw an exception if the
# file exists but is not readable.  Return undefined (false) if the file is
# not there.

sub check_output {

    my ($self, $template) = @_;
    
    return unless defined $template && $template ne '';
    
    my $template_file = $self->{output_templates} . '/' . $template;
    
    if ( -e $template_file )
    {
	return $template if -r $template_file;
	croak "template $template_file: $!";
    }
    
    return;
}


# render_output ( template, definitions, header, footer, vars )
# 
# Render the specified template, with the specified parameters.  If
# $definitions and/or $header are defined, process them before the main
# template.  If $footer is defined, process it after the main template.  If
# $vars is defined, it must be a hashref of variable definitions that will be
# passed to the template rendering engine.

sub render_output {
    
    my $self = shift;
    
    # Throw an exception if no templating module was selected.
    
    croak "you must select a templating module"
	unless defined $self->{templating_plugin};
    
    $self->{templating_plugin}->render_template($self->{doc_templates}, @_);
}


1;
