# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs

PATH=$PATH:$HOME/bin
EDITOR=/usr/bin/vim
CVSROOT=/home/cvsroot

export PATH EDITOR CVSROOT
unset USERNAME

PS1='\u@\h \w:$'
PS2='> '
