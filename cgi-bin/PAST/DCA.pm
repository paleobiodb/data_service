package PAST::DCA;
use strict;
use GD;

#    GLOBAL $ERR

sub sqr {
$_[0] * $_[0];
}

#sub QUIKIN(MMAX,NMAX,NDAT: integer; var MM, N: integer;
#  var IDAT,QIDAT,IX4,IEND: variant; NID: integer; var INFLAG: variant);
sub QUIKIN
{
  my ($MMAX, $NMAX, $NDAT, $MM, $N, $IDAT, $QIDAT, $IX4, $IEND,
    $NID, $INFLAG, $dcarr) = @_;

  my $ID=0; my $NITEM=0; my $I=0; my $II=0; my $III=0;
  my $IT=0; my $J=0; my $ITT=0;
  my $AIJ=0.0;
  my @ITEM=(); # integers
  my @AITEM=(); # doubles

#  GetMem(item, ($NDAT+1)*SizeOf(integer));
#  GetMem(aitem, ($NDAT+1)*SizeOf(double));

#for ($I=0; $I<$PAST::DCA::dcaM; $I++) {
#  for ($J=0; $J<$PAST::DCA::dcaN; $J++) {
#    print(${$dcarr}[$I][$J]);
#  }
#  print("<br>");
#}

  $NITEM=$PAST::DCA::dcaM;
  for ($II=1; $II<=$MMAX; $II++) { ${$INFLAG}[$II]=0; } # Flag for samples table
  $ID=0; # IDAT and QIDAT counter
  ${$N}=0; # Species (row) counter
  $III=1; # Last sample number
  $ITEM[$NITEM+1]=0;
  $II=0; # Added by me (sample number)
L40: $II++; # Sample number (added by me)
  if ($II<=$PAST::DCA::dcaN) {
    $ITT=0;
    for ($IT=1; $IT<=$NITEM; $IT++) { # Go down the sample column non-zero (added by me)
      if (${$dcarr}[$IT-1][$II-1]>0) {
        $ITT++; # Added by me
        $ITEM[$ITT]=$IT; # Species number
        $AITEM[$ITT]=${$dcarr}[$IT-1][$II-1];
        $ITEM[$ITT+1]=0; $AITEM[$ITT+1]=0;
     }
    }
  }
  if (($ITT==0) and ($II<=$PAST::DCA::dcaN)) { goto(L40); } # Added in ver. 1.23
  ${$IEND}[$III]=$ID;
  if ($II>$PAST::DCA::dcaN) { goto L100; }
  ${$IX4}[$II]=$ID+1;
  $III=$II;
L50: $IT=0;
L55: $IT++;
  $J=$ITEM[$IT];
  if ($J<0) {
    print("Negative number in sample");
#    freemem($ITEM); freemem($AITEM);
    $PAST::DCA::ERR=1;
    return;
  }
  if ($J==0) { goto L40; }

  if ($J>${$N}) { ${$N}=$J; }
  ${$INFLAG}[$II]=1;
  $ID++;
  ${$IDAT}[$ID]=$J;
  $AIJ=$AITEM[$IT];
  if ($AIJ<0.0) {
    print("Negative number in sample");
#    freemem($ITEM); freemem($AITEM);
    $PAST::DCA::ERR=1;
    return;
  }
  ${$QIDAT}[$ID]=$AIJ;
#  if ($ID>$NDAT) { (* Was NDAT-3 *)
#    write('No more space for (data');
#    freemem(item); freemem(aitem);
#    $PAST::DCA::ERR=1;
#    return;
#  }
  goto L55;
L100: ${$MM}=$III;
  if (${$MM}>$MMAX) {
    write('Max number of columns exceeded');
#    freemem(item); freemem(aitem);
    $PAST::DCA::ERR=1;
    return;
  }
  if (${$N}>$NMAX) {
    print("Max number of rows ".+$NMAX." exceeded (".$N.")");
#    freemem(item); freemem(aitem);
    $PAST::DCA::ERR=1;
    return;
  }
  $NID=$ID;
  ${$IX4}[1]=1;

  $I=0;
  for ($II=1; $II<=${$MM}; $II++) {
    if (${$INFLAG}[$II]==1) {
      $I++;
      ${$IX4}[$I]=${$IX4}[$II];
      ${$IEND}[$I]=${$IEND}[$II];
      ${$INFLAG}[$I]=$II;
    }
  }
  ${$MM}=$I;
#  freemem(item); freemem(aitem);
}

#sub XMAXMI(var X: variant; var AXMAX,AXMIN: real; M: integer);
sub XMAXMI
# FORMS MAXIMUM AND MINIMUM OF X(M)
#      REAL X(M)
{
  my ($X, $AXMAX, $AXMIN, $M) = @_;

  my $I=0; my $AX=0.0;

  ${$AXMAX}=-1.0E10;
  ${$AXMIN}=-${$AXMAX};
  for ($I=1; $I<=$M; $I++) {
    $AX=${$X}[$I];
    if ($AX>${$AXMAX}) { ${$AXMAX}=$AX; }
    if ($AX<${$AXMIN}) { ${$AXMIN}=$AX; }
  }
}

#sub XYMULT(var X,Y: variant; MI, N, NID: integer;
#  var IX4, IEND, IDAT, QIDAT: variant);
sub XYMULT
# STARTS WITH VECTOR X AND FORMS MATRIX PRODUCT Y=AX
#      REAL X(MI),Y(N),QIDAT(NID)
#      INTEGER IX4(MI),IEND(MI),IDAT(NID)
{
  my ($X, $Y, $MI, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT)=@_;

  my $I=0; my $J=0; my $ID=0; my $ID1=0; my $ID2=0;
  my $AX=0.0;

  for ($J=1; $J<=$N; $J++) { ${$Y}[$J]=0.0; }
  for ($I=1; $I<=$MI; $I++) {
    $ID1=${$IX4}[$I];
    $ID2=${$IEND}[$I];
    $AX=${$X}[$I];
    for ($ID=$ID1; $ID<=$ID2; $ID++) {
      $J=${$IDAT}[$ID];
      ${$Y}[$J]=${$Y}[$J]+$AX*${$QIDAT}[$ID];
    }
  }
}

#sub YXMULT(var Y,X: variant; MI,N,NID: integer;
#  var IX4,IEND,IDAT,QIDAT: variant);
sub YXMULT
# STARTS WITH VECTOR Y AND FORMS MATRIX PRODUCT X=AY
#      REAL X(MI),Y(N),QIDAT(NID)
#      INTEGER IX4(MI),IEND(MI),IDAT(NID)
{
  my ($Y, $X, $MI, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT) = @_;

  my $I=0; my $J=0; my $ID=0; my $ID1=0; my $ID2=0;
  my $AX=0.0;
  
  for ($I=1; $I<=$MI; $I++) {
    $ID1=${$IX4}[$I];
    $ID2=${$IEND}[$I];
    $AX=0.0;
    for ($ID=$ID1; $ID<=$ID2; $ID++) {
      $J=${$IDAT}[$ID];
      $AX=$AX+${$Y}[$J]*${$QIDAT}[$ID];
    }
    ${$X}[$I]=$AX;
  }
}

#sub CUTUP(var X, IX: variant; MI, MK: integer);
sub CUTUP
# TAKES A VECTOR X AND CUTS UP INTO (MK-4) SEGMENTS, PUTTING A
# SEGMENTED VERSION OF THE VECTOR IN IX.
#      REAL X(MI)
#      INTEGER IX(MI)
{
  my ($X, $IX, $MI, $MK) = @_;

  my $I=0; my $MMK=0; my $MAXK=0; my $IAX=0;
  my $AXMIN=0.0; my $AXMAX=0.0; my $AXBIT=0.0;

  $MMK=$MK-4;
  $MAXK=$MK-2;
  XMAXMI($X, \$AXMAX, \$AXMIN, $MI);
  $AXBIT=($AXMAX-$AXMIN)/$MMK;
  for ($I=1; $I<=$MI; $I++) {
    $IAX=int((${$X}[$I]-$AXMIN)/$AXBIT)+3;
    if ($IAX<3) { $IAX=3; }
    if ($IAX>$MAXK) { $IAX=$MAXK; }
    ${$IX}[$I]=$IAX;
  }
}

#sub DETRND(var X,AIDOT,IX: variant; MI,MK: integer);
#      REAL X(MI),Z(50),ZN(50),ZBAR(50),AIDOT(MI)
sub DETRND
#      INTEGER IX(MI)
# STARTS WITH A VECTOR X AND DETRENDS WITH RESPECT TO GROUPS DEFINED
# BY IX.  DETRENDING IS IN BLOCKS OF 3 UNITS AT A TIME, AND THE
# RESULT CALCULATED IS THE AVERAGE OF THE 3 POSSIBLE RESULTS THAT
# CAN BE OBTAINED, CORRESPONDING TO 3 POSSIBLE STARTING POSITIONS
# FOR THE BLOCKS OF 3.
#var Z, ZN, ZBAR: array[1..100] of real;
{
  my ($X, $AIDOT, $IX, $MI, $MK) = @_;

  my $I=0; my $K=0; my $MMK=0;
  my @Z=(); my @ZN=(); my @ZBAR=();

  for ($K=1; $K<=$MK; $K++) {
    $Z[$K]=0.0;
    $ZN[$K]=0.0
  }
  for ($I=1; $I<=$MI; $I++) {
    $K=${$IX}[$I];
    $Z[$K]=$Z[$K]+${$X}[$I]*${$AIDOT}[$I];
    $ZN[$K]=$ZN[$K]+${$AIDOT}[$I];
  }
  $MMK=$MK-1;
  for ($K=2; $K<=$MMK; $K++) {
    $ZBAR[$K]=($Z[$K-1]+$Z[$K]+$Z[$K+1])/($ZN[$K-1]+$ZN[$K]+$ZN[$K+1]+1.0E-12); }
  $MMK=$MMK-1;
  for ($K=3; $K<=$MMK; $K++) {
    $Z[$K]=($ZBAR[$K-1]+$ZBAR[$K]+$ZBAR[$K+1])/3.0; }
  for ($I=1; $I<=$MI; $I++) {
    $K=${$IX}[$I];
    ${$X}[$I]=${$X}[$I]-$Z[$K];
  }
}

#sub TRANS(var Y,YY, X: variant;
#     NEIG, IRA: integer; var AIDOT,XEIG1,XEIG2,XEIG3,IX1,IX2,IX3: variant;
#     MI,MK,N,NID: integer; var IX4,IEND,IDAT,QIDAT: variant);
sub TRANS
# THIS SUBROUTINE IS THE CRUX OF THE WHOLE PROGRAM, IN THAT IT
# TAKES A SET OF SPECIES SCORES Y AND ITERATES TO FIND A NEW SET
# OF SCORES YY.  REPEATED ITERATION OF THIS SUBROUTINE WOULD LEAD
# EVENTUALLY TO THE CORRECT SOLUTION (EXCEPT THAT THE SCORES NEED
# TO BE DIVIDED BY THE Y-TOTALS ADOTJ AT EACH ITERATION).  THE
# CALLING PROGRAM EIGY IS MADE LENGTHY BY SOME FANCY ALGEBRA PUT
# THERE TO SPEED UP THE CALCULATION.  ESSENTIALLY TRANS IS THE
# STANDARD RECIPROCAL AVERAGING ITERATION WITH EITHER DETRENDING
# WITH RESPECT TO PREVIOUSLY DERIVED AXES (IN THE CASE OF DETRENDED
# CORRESPONDENCE ANALYSIS) OR ORTHOGONALIZATION WITH RESPECT TO
# THEM (IN THE CASE OF RECIPROCAL AVERAGING).
#      REAL X(MI),XEIG1(MI),XEIG2(MI),XEIG3(MI)
#      REAL Y(N),YY(N),AIDOT(MI),QIDAT(NID)
#      INTEGER IX1(MI),IX2(MI),IX3(MI),IDAT(NID),IX4(MI),IEND(MI)
{
  my ($Y, $YY, $X, $NEIG, $IRA, $AIDOT, $XEIG1, $XEIG2, $XEIG3,
    $IX1, $IX2, $IX3, $MI, $MK, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT) = @_;

  my $I=0;
  my $A1=0.0; my $A2=0.0; my $A3=0.0;

  YXMULT($Y, $X, $MI, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT);
  for ($I=1; $I<=$MI; $I++) { ${$X}[$I]=${$X}[$I]/${$AIDOT}[$I]; }
  if ($NEIG==0) { goto LL200; }
  if ($IRA==1) { goto LL100; }
  DETRND($X, $AIDOT, $IX1, $MI, $MK);
  if ($NEIG==1) { goto LL200; }
  DETRND($X, $AIDOT, $IX2, $MI, $MK);
  if ($NEIG!=2) {
    DETRND($X, $AIDOT, $IX3, $MI, $MK);
    DETRND($X, $AIDOT, $IX2, $MI, $MK);
  }
  DETRND($X, $AIDOT, $IX1, $MI, $MK);
  goto LL200;
LL100: $A1=0.0;
  for ($I=1; $I<=$MI; $I++) { $A1=$A1+${$AIDOT}[$I]*${$X}[$I]*${$XEIG1}[$I]; }
  for ($I=1; $I<=$MI; $I++) { ${$X}[$I]=${$X}[$I]-$A1*${$XEIG1}[$I];}
  if ($NEIG==1) { goto LL200; }
  $A2=0.0;
  for ($I=1; $I<=$MI; $I++) { $A2=$A2+${$AIDOT}[$I]*${$X}[$I]*${$XEIG2}[$I]; }
  for ($I=1; $I<=$MI; $I++) { ${$X}[$I]=${$X}[$I]-$A2*${$XEIG2}[$I]; }
  if ($NEIG==2) { goto LL200; }
  $A3=0.0;
  for ($I=1; $I<=$MI; $I++) { $A3=$A3+${$AIDOT}[$I]*${$X}[$I]*${$XEIG3}[$I]; }
  for ($I=1; $I<=$MI; $I++) { ${$X}[$I]=${$X}[$I]-$A3*${$XEIG3}[$I]; }
LL200: XYMULT($X, $YY, $MI, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT);
}

#sub SEGMNT(var X, Y, ZN, ZV: variant; MI, MK, N, NID: integer;
#  var AIDOT,IX4,IEND,IDAT,QIDAT: variant);
sub SEGMNT
# GIVEN AN ORDINATION (X,Y), CALCULATES NUMBERS AND SUMMED MEAN-SQUARE
#DEVIATIONS IN MK SEGMENTS.  ZN(K) IS THE NUMBER OF SAMPLES IN SEGMENT
#K;  ZV(K) IS THE SUMMED MEAN-SQUARE DEVIATION.  (WE AIM TO MAKE ZV,
#ZN AS NEARLY EQUAL AS POSSIBLE.)
#      REAL X(MI),Y(N),ZN(MK),ZV(MK),AIDOT(MI),QIDAT(NID)
#      INTEGER IX4(MI),IEND(MI),IDAT(NID)
{
  my ($X, $Y, $ZN, $ZV, $MI, $MK, $N, $NID,
    $AIDOT, $IX4,$IEND, $IDAT, $QIDAT) = @_;

  my $I=0; my $J=0; my $K=0; my $ID=0; my $ID1=0; my $ID2=0;
  my $AIJ=0.0; my $AX=0.0; my $SUMSQ=0.0; my $SQCORR=0.0;
  my $AXMAX=0.0; my $AXMIN=0.0; my $AXBIT=0.0;

  for ($K=1; $K<=$MK; $K++) {
    ${$ZN}[$K]=-1.0E-20;
    ${$ZV}[$K]=-1.0E-20;
  }
  XMAXMI($X, \$AXMAX, \$AXMIN, $MI);
  $AXBIT=($AXMAX-$AXMIN)/$MK;
  for ($I=1; $I<=$MI; $I++) { ${$X}[$I]=${$X}[$I]-$AXMIN; }
  for ($J=1; $J<=$N; $J++) { ${$Y}[$J]=${$Y}[$J]-$AXMIN; }
  for ($I=1; $I<=$MI; $I++) {
    $SQCORR=0.0;
    $SUMSQ=2.0E-20;
    $ID1=${$IX4}[$I];
    $ID2=${$IEND}[$I];
    $AX=${$X}[$I];
    for ($ID=$ID1; $ID<=$ID2; $ID++) {
      $J=${$IDAT}[$ID];
      $AIJ=${$QIDAT}[$ID];
      $SQCORR=$SQCORR+sqr($AIJ);
      $SUMSQ=$SUMSQ+$AIJ*sqr($AX-${$Y}[$J]);
    }
    $SQCORR=$SQCORR/sqr(${$AIDOT}[$I]);
    if ($SQCORR>0.9999) { $SQCORR=0.9999; }
    $SUMSQ=$SUMSQ/${$AIDOT}[$I];
    $K=int($AX/$AXBIT)+1;
    if ($K>$MK) { $K=$MK; }
    if ($K<1) { $K=1; }
    ${$ZV}[$K]=${$ZV}[$K]+$SUMSQ;
    ${$ZN}[$K]=${$ZN}[$K]+1.0-$SQCORR;
  }
}

#sub SMOOTH(var Z: variant; MK: integer);
sub SMOOTH
#      REAL Z(MK)
#TAKES A VECTOR Z AND DOES (1,2,1)-SMOOTHING UNTIL NO BLANKS LEFT
#AND THEN 2 MORE ITERATIONS OF (1,2,1)-SMOOTHING.  IF NO BLANKS TO
#END WITH, THEN DOES 3 SMOOTHINGS, I.E. EFFECTIVELY (1,6,15,20,
#15,6,1)-SMOOTHING.
{
  my ($Z, $MK) = @_;

  my $ISTOP=0; my $ICOUNT=0; my $K3=0;
  my $AZ1=0.0; my $AZ2=0.0; my $AZ3=0.0;

  $ISTOP=1;
  for ($ICOUNT=1; $ICOUNT<=50; $ICOUNT++) {
    $AZ2=${$Z}[1];
    $AZ3=${$Z}[2];
    if ($AZ3==0.0) { $ISTOP=0; }
    ${$Z}[1]=0.75*$AZ2+0.25*$AZ3;
    for ($K3=3; $K3<=$MK; $K3++) {
      $AZ1=$AZ2;
      $AZ2=$AZ3;
      $AZ3=${$Z}[$K3];
      if ($AZ3<=0.0) { $ISTOP=0; }
      ${$Z}[$K3-1]=0.5*($AZ2+0.5*($AZ1+$AZ3));
    }
    ${$Z}[$MK]=0.25*$AZ2+0.75*$AZ3;
    $ISTOP++;
    if ($ISTOP==4) { return; }
  }
}

#sub STRTCH(var X,Y: variant; SHORT: real;
#  MONIT,MI,N,NID: integer; var AIDOT,IX4,IEND,IDAT,QIDAT: variant);
sub STRTCH
# TAKES AN AXIS (X,Y) AND SCALES TO UNIT MEAN SQUARE DEV OF SPECIES
# SCORES PER SAMPLE.  AN ATTEMPT IS MADE for (LONGER AXES (L > SHORT)
# TO SQUEEZE THEM IN AND OUT SO THAT THEY HAVE THE RIGHT MEAN SQUARE
# DEVIATION ALL THE WAY ALONG THE AXIS AND NOT ONLY ON AVERAGE.
#      REAL X(MI),Y(N),AIDOT(MI),QIDAT(NID)
#      REAL ZN(50),ZV(50)
#      INTEGER IX4(MI),IEND(MI),IDAT(NID)
#--COMMON BLOCK ADDED BY P.MINCHIN FEB 1988
#      COMMON /LUNITS/ IUINP1,IUINP2,IUOUT1,IUOUT2,IUOUT3
#var zn, zv: variant;
{
  my ($X, $Y, $SHORT, $MONIT, $MI, $N, $NID,
    $AIDOT, $IX4, $IEND, $IDAT, $QIDAT) = @_;

  my $ICOUNT=0; my $MK=0; my $K=0; my $I=0; my $J=0; my $IAY=0;
  my $AZV=0.0; my $ZVSUM=0.0; my $SD=0.0; my $ALONG=0.0;
  my $AX=0.0; my $AZ=0.0; my $AXBIT=0.0;
  my @ZN=(); my @ZV=();

#  zn=vararraycreate([1,100], vardouble);
#  zv=vararraycreate([1,100], vardouble);
  for ($ICOUNT=1; $ICOUNT<=2; $ICOUNT++) {
    $MK=20;
    SEGMNT($X, $Y, \@ZN, \@ZV, $MI, $MK, $N, $NID,
      $AIDOT, $IX4, $IEND, $IDAT, $QIDAT);
    SMOOTH(\@ZV, $MK);
    SMOOTH(\@ZN, $MK);
    $ZVSUM=0.0;
    for ($K=1; $K<=$MK; $K++) { $ZVSUM=$ZVSUM+$ZV[$K]/$ZN[$K]; }
# zvsum is tiny negative here.
  $ZVSUM=abs($ZVSUM); # Added by me
    $SD=sqrt($ZVSUM/$MK);
# WE WANT MEAN WITHIN-SAMPLE SQUARE DEVIATION TO BE 1.0, SO WE DIVIDE
# EVERYTHING BY SD
    $ALONG=0.0;
    for ($I=1; $I<=$MI; $I++) {
      $AX=${$X}[$I]/$SD;
      ${$X}[$I]=$AX;
      if ($ALONG<$AX) { $ALONG=$AX; }
    };
#    if ((ICOUNT==1) AND (MONIT==1)) then WRITE(IUOUT2,1000)
# 1000 FORMAT(/1X)
#    if (MONIT=1) then WRITE(IUOUT2,1001) ALONG
# 1001 FORMAT(1X,'LENGTH OF GRADIENT',F10.3)
#    gradlength=ALONG;
    for ($J=1; $J<=$N; $J++) { ${$Y}[$J]=${$Y}[$J]/$SD; }
    if ($ALONG<$SHORT) { return; }
    if ($ICOUNT==2) { return; }
    $MK=int($ALONG*5.0)+1;
    if ($MK<10) { $MK=10; }
    if ($MK>45) { $MK=45; }
    SEGMNT($X, $Y, \@ZN, \@ZV, $MI, $MK, $N, $NID,
      $AIDOT, $IX4, $IEND, $IDAT, $QIDAT);
    SMOOTH(\@ZV,$MK);
    SMOOTH(\@ZN,$MK);
    $ZVSUM=0.0;
    for ($K=1; $K<=$MK; $K++) {
      $AZV=1.0/sqrt(0.2/$ALONG+$ZV[$K]/$ZN[$K]);
      $ZVSUM=$ZVSUM+$AZV;
      $ZV[$K]=$AZV;
    }
    for ($K=1; $K<=$MK; $K++) { $ZV[$K]=$ZV[$K]*$ALONG/$ZVSUM; }
    $AZ=0.0;
    $ZN[1]=0.0;
    for ($K=1; $K<=$MK; $K++) {
      $AZ=$AZ+$ZV[$K];
      $ZN[$K+1]=$AZ;
    }
    $AXBIT=$ALONG/$MK;
    for ($J=1; $J<=$N; $J++) {
      $IAY=int(${$Y}[$J]/$AXBIT)+1;
      if ($IAY<1) { $IAY=1; }
      if ($IAY>$MK) { $IAY=$MK; }
      ${$Y}[$J]=$ZN[$IAY]+$ZV[$IAY]*(${$Y}[$J]/$AXBIT-($IAY-1));
    }
    YXMULT($Y, $X, $MI, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT);
    for ($I=1; $I<=$MI; $I++) { ${$X}[$I]=${$X}[$I]/${$AIDOT}[$I]; }
  }
}

#sub EIGY(var X,Y: variant; var EIG: real; NEIG,IRA,IRESC: integer;
#  SHORT: real; MI, MK, N, NID: integer;
#  var IX4,IEND,IDAT,QIDAT,Y2,Y3,Y4,Y5,
#  XEIG1,XEIG2,XEIG3,IX1,IX2,IX3,AIDOT,ADOTJ: variant): integer;
sub EIGY
# EXTRACTS AN EIGENVECTOR Y WITH EIGENVALUE EIG.  THE ALGEBRA
# IS A LITTLE COMPLICATED, BUT CONSISTS ESSENTIALLY OF REPRE-
# SENTING THE TRANSFORMATION (SUBROUTINE TRANS) APPROXIMATELY
# BY A TRIDIAGONAL 4X4 MATRIX.  THE EIGENPROBLEM for (THE
# TRIDIAGONAL MATRIX IS SOLVED AND THIS SOLUTION IS PLUGGED
# BACK IN TO OBTAIN A NEW TRIAL VECTOR.
# AFTER GETTING THE EIGENVECTOR, THE SCORES MAY BE RESCALED
# (SUBROUTINE STRTCH).
#      REAL X(MI),Y(N),Y2(N),Y3(N),Y4(N),Y5(N)
#      REAL XEIG1(MI),XEIG2(MI),XEIG3(MI),AIDOT(MI),ADOTJ(N)
#      REAL QIDAT(NID)
#      INTEGER IX4(MI),IEND(MI),IDAT(NID),IX1(MI),IX2(MI),IX3(MI)
{
  my ($X, $Y, $EIG, $NEIG, $IRA, $IRESC, $SHORT, $MI, $MK, $N, $NID,
    $IX4, $IEND, $IDAT, $QIDAT, $Y2, $Y3, $Y4, $Y5,
    $XEIG1, $XEIG2, $XEIG3, $IX1, $IX2, $IX3, $AIDOT, $ADOTJ) = @_;

  my $ITIMES=0; my $I=0; my $J=0; my $ID=0; my $ICOUNT=0;
  my $ITEMS=0; my $ID1=0; my $ID2=0; my $MONIT=0;
  my $A=0.0; my $A11=0.0; my $A12=0.0; my $A22=0.0; my $A23=0.0;
  my $A33=0.0; my $A34=0.0; my $A44=0.0; my $AY=0.0; my $EX=0.0;
  my $EXX=0.0; my $TOT=0.0; my $TOL=0.0; my $B13=0.0;
  my $B14=0.0; my $B24=0.0; my $AX1=0.0; my $AX2=0.0;
  my $AX3=0.0; my $AX4=0.0; my $AXX1=0.0; my $AXX2=0.0;
  my $AXX3=0.0; my $AXX4=0.0; my $RESI=0.0; my $SIGN=0.0;
  my $AYMIN=0.0; my $AYMAX=0.0; my $AXLONG=0.0; my $SUMSQ=0.0;
  my $SD=0.0; my $SD1=0.0; my $AX=0.0;


  $TOT=0.0;
  for ($J=1; $J<=$N; $J++) {
    $TOT=$TOT+${$ADOTJ}[$J];
    ${$Y}[$J]=$J;
  }
  if ($TOT==0) { return 1; }
  ${$Y}[1]=1.1;
#---Tolerance reduced by P.Minchin Jan 1997
#      TOL=0.0001
  $TOL=0.000005;
  TRANS($Y, $Y, $X, $NEIG, $IRA, $AIDOT, $XEIG1, $XEIG2, $XEIG3,
    $IX1, $IX2, $IX3, $MI, $MK, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT);
  $ICOUNT=0;
LLL20: $A=0.0;
  for ($J=1; $J<=$N; $J++) { $A=$A+${$Y}[$J]*${$ADOTJ}[$J]; }
  $A=$A/$TOT;
  $EX=0.0;
  for ($J=1; $J<=$N; $J++) {
    $AY=${$Y}[$J]-$A;
    $EX=$EX+$AY*$AY*${$ADOTJ}[$J];
    ${$Y}[$J]=$AY;
  }
  $EX=sqrt($EX);
  if ($EX==0) { return 1; }
  for ($J=1; $J<=$N; $J++) { ${$Y}[$J]=${$Y}[$J]/$EX; }
  TRANS($Y, $Y2, $X, $NEIG, $IRA, $AIDOT, $XEIG1, $XEIG2, $XEIG3,
    $IX1,$IX2, $IX3, $MI, $MK, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT);
  $A=0.0;
  $A11=0.0;
  $A12=0.0;
  $A22=0.0;
  $A23=0.0;
  $A33=0.0;
  $A34=0.0;
  $A44=0.0;
  for ($J=1; $J<=$N; $J++) {
    $AY=${$Y2}[$J];
    if (${$ADOTJ}[$J]==0) { return 1; }
    ${$Y2}[$J]=$AY/${$ADOTJ}[$J];
    $A=$A+$AY;
    $A11=$A11+$AY*${$Y}[$J];
  }
  $A=$A/$TOT;
  for ($J=1; $J<=$N; $J++) {
    $AY=${$Y2}[$J]-($A+$A11*${$Y}[$J]);
    $A12=$A12+$AY*$AY*${$ADOTJ}[$J];
    ${$Y2}[$J]=$AY;
  }
  $A12=sqrt($A12);
  if ($A12==0) { return 1; }
  for ($J=1; $J<=$N; $J++) { ${$Y2}[$J]=${$Y2}[$J]/$A12; }
#  IF (ICOUNT=0) then WRITE(1000);
#      WRITE(IUOUT2,1011) A12,ICOUNT
# 1011 FORMAT(1X,'RESIDUAL',F10.6,'       AT ITERATION',I3)
      if ($A12<$TOL) { goto LLL200; }
#--Maximum iteration limit increased by P.Minchin Jan 1997
      if ($ICOUNT>999) { goto LLL200; }
  $ICOUNT++;
  TRANS($Y2, $Y3, $X, $NEIG, $IRA, $AIDOT, $XEIG1, $XEIG2, $XEIG3,
    $IX1, $IX2, $IX3, $MI, $MK, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT);
  $A=0.0;
  $B13=0.0;
  for ($J=1; $J<=$N; $J++) {
    $AY=${$Y3}[$J];
    ${$Y3}[$J]=$AY/${$ADOTJ}[$J];
    $A=$A+$AY;
    $A22=$A22+$AY*${$Y2}[$J];
    $B13=$B13+$AY*${$Y}[$J];
  }
  $A=$A/$TOT;
  for ($J=1; $J<=$N; $J++) {
    $AY=${$Y3}[$J]-($A+$A22*${$Y2}[$J]+$B13*${$Y}[$J]);
    $A23=$A23+$AY*$AY*${$ADOTJ}[$J];
    ${$Y3}[$J]=$AY;
  }
  $A23=sqrt($A23);
  if ($A23<=$TOL) {
    $A23=0.0;
    goto LLL160;
  }
  for ($J=1; $J<=$N; $J++) { ${$Y3}[$J]=${$Y3}[$J]/$A23; }
  TRANS($Y3, $Y4, $X, $NEIG, $IRA, $AIDOT, $XEIG1, $XEIG2, $XEIG3,
    $IX1, $IX2, $IX3, $MI, $MK, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT);
  $A=0.0;
  $B14=0.0;
  $B24=0.0;
  for ($J=1; $J<=$N; $J++) {
    $AY=${$Y4}[$J];
    if (${$ADOTJ}[$J]==0) { return 1; }
    ${$Y4}[$J]=${$Y4}[$J]/${$ADOTJ}[$J];
    $A=$A+$AY;
    $A33=$A33+$AY*${$Y3}[$J];
    $B14=$B14+$AY*${$Y}[$J];
    $B24=$B24+$AY*${$Y2}[$J];
  }
  $A=$A/$TOT;
  for ($J=1; $J<=$N; $J++) {
    $AY=${$Y4}[$J]-($A+$A33*${$Y3}[$J]+$B14*${$Y}[$J]+$B24*${$Y2}[$J]);
    $A34=$A34+$AY*$AY*${$ADOTJ}[$J];
    ${$Y4}[$J]=$AY;
  }
  $A34=sqrt($A34);
  if ($A34<=$TOL) {
    $A34=0.0;
    GOTO LLL160;
  }
  if ($A34==0) { return 1; }
  for ($J=1; $J<=$N; $J++) { ${$Y4}[$J]=${$Y4}[$J]/$A34; }
  TRANS($Y4 , $Y5, $X, $NEIG, $IRA, $AIDOT, $XEIG1, $XEIG2, $XEIG3,
    $IX1, $IX2, $IX3, $MI, $MK, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT);
  for ($J=1; $J<=$N; $J++) { $A44=$A44+${$Y4}[$J]*${$Y5}[$J]; }
# WE NOW HAVE THE TRIDIAGONAL REPRESENTATION OF TRANS.  SOLVE
# EIGENPROBLEM FOR TRIDIAGONAL MATRIX.
LLL160: $AX1=1.0;
  $AX2=0.1;
  $AX3=0.01;
  $AX4=0.001;
  for ($ITIMES=1; $ITIMES<=100; $ITIMES++) {
    $AXX1=$A11*$AX1+$A12*$AX2;
    $AXX2=$A12*$AX1+$A22*$AX2+$A23*$AX3;
    $AXX3=$A23*$AX2+$A33*$AX3+$A34*$AX4;
    $AXX4=$A34*$AX3+$A44*$AX4;
    $AX1=$A11*$AXX1+$A12*$AXX2;
    $AX2=$A12*$AXX1+$A22*$AXX2+$A23*$AXX3;
    $AX3=$A23*$AXX2+$A33*$AXX3+$A34*$AXX4;
    $AX4=$A34*$AXX3+$A44*$AXX4;
    $EX=sqrt(sqr($AX1)+sqr($AX2)+sqr($AX3)+sqr($AX4));
    if ($EX==0) { return 1; }
    $AX1=$AX1/$EX;
    $AX2=$AX2/$EX;
    $AX3=$AX3/$EX;
    $AX4=$AX4/$EX;
    if ($ITIMES/5.0==int($ITIMES/5.0)) { next; } # was continue in Delphi
    $EXX=sqrt($EX);
    $RESI=sqrt(sqr($AX1-$AXX1/$EXX)+sqr($AX2-$AXX2/$EXX)+
      sqr($AX3-$AXX3/$EXX)+sqr($AX4-$AXX4/$EXX));
    if ($RESI<$TOL*0.05) { last; } # was break in Delphi
  }
  for ($J=1; $J<=$N; $J++) {
    ${$Y}[$J]=$AX1*${$Y}[$J]+$AX2*${$Y2}[$J]+$AX3*${$Y3}[$J]+$AX4*${$Y4}[$J]; }
  goto LLL20;
LLL200: # WRITE(IUOUT2,1010) A11
# 1010 FORMAT(1X,'EIGENVALUE',F10.5)
#      IF(A12>TOL) WRITE(IUOUT2,1012) TOL
#      IF(A12>TOL) WRITE(IUOUT1,1012) TOL
#MessageDlg('*** BEWARE ***     RESIDUAL BIGGER THAN TOLERANCE',
#  mtwarning, [mbabort], 0);
# WE CALCULATE X FROM Y, AND SET X TO UNIT LENGTH IF RECIPROCAL
# AVERAGING OPTION IS IN FORCE (IRA=1)
  XMAXMI($Y, \$AYMAX, \$AYMIN, $N);
  $SIGN=1.0;
  if (-$AYMIN>$AYMAX) { $SIGN=-1.0; }
  for ($J=1; $J<=$N; $J++) { ${$Y}[$J]=${$Y}[$J]*$SIGN; }
  YXMULT($Y, $X, $MI, $N, $NID, $IX4, $IEND, $IDAT, $QIDAT);
  for ($I=1; $I<=$MI; $I++) {
    if (${$AIDOT}[$I]==0) { return 1; }
    ${$X}[$I]=${$X}[$I]/${$AIDOT}[$I];
  }
  if ($IRESC==0) { goto LLL225; }
  if ($A11>0.999) { goto LLL225; }
  for ($I=1; $I<=$IRESC; $I++) {
    $MONIT=0;
    if (($I==1) or ($I==$IRESC)) { $MONIT=1; }
    STRTCH($X, $Y, $SHORT, $MONIT, $MI, $N,
      $NID, $AIDOT, $IX4, $IEND, $IDAT, $QIDAT);
  }
  ${$EIG}=$A11;
  return 0;
LLL225: $AXLONG=0.0;
  for ($I=1; $I<=$MI; $I++) { $AXLONG=$AXLONG+${$AIDOT}[$I]*sqr(${$X}[$I]); }
  $AXLONG=sqrt($AXLONG);
  if ($AXLONG==0) { return 1; }
  for ($I=1; $I<=$MI; $I++) {  ${$X}[$I]=${$X}[$I]/$AXLONG; }
  for ($J=1; $J<=$N; $J++) {  ${$Y}[$J]=${$Y}[$J]/$AXLONG ; }
# IT REMAINS TO SCALE Y TO UNIT WITHIN-SAMPLE STANDARD DEVIATION
  $SUMSQ=0.0;
  for ($I=1; $I<=$MI; $I++) {
    $ID1=${$IX4}[$I];
    $ID2=${$IEND}[$I];
    $AX=${$X}[$I];
    for ($ID=$ID1; $ID<=$ID2; $ID++) {
      $J=${$IDAT}[$ID];
      $SUMSQ=$SUMSQ+${$QIDAT}[$ID]*sqr($AX-${$Y}[$J]);
    }
  }
  if (($TOT==0) or ($AXLONG==0)) { return 1; }
  $SD=sqrt($SUMSQ/$TOT);
  if ($A11>0.999) {
    $SD=$AYMAX/$AXLONG;
    $SD1=-$AYMIN/$AXLONG;
    if ($SD1>$SD) { $SD=$SD1; }
  }
  if ($SD==0) { return 1; }
  for ($J=1; $J<=$N; $J++) { ${$Y}[$J]=${$Y}[$J]/$SD; }
  ${$EIG}=$A11;
  return 0;
}


sub dca {
  my ($data_trans,$rows,$columns,$extra_data) = @_;
  my @rows = @{$rows};
  my @columns = @{$columns};
  my %extra_data = %{$extra_data};

  # need field names from extra_data because they index the hash array
  # using rows 0 because any collection number will do
  my @extra_fields = keys %{$extra_data{$rows[0]}};

#print("DCA START<br>");

#CONST MAXSAM=5000; MAXSPP=5000; MAXDAT=330000; # Set according to matrix!
#    BEFORE,AFTER: array[1..50] of real;
#--REAL ARRAY SCORES(8) ADDED BY P.MINCHIN FEB 1988 (FOR CONFIG OUTPUT)
#    SCORES: array[1..8] of real;
#--CHARACTER VARIABLES FOR FILE NAMES ADDED BY P.MINCHIN MAY 1992
#    DATFIL, PRTFIL, OUTFIL: string;
#--CHARACTER VARIABLE FOR COMMAND LINE ADDED P.Minchin June 1997
#    CMLINE: string;
#      COMMON IDAT,QIDAT  doesn't seem to be used
#--Common block added by P.Minchin May 1997
#      COMMON /MAXDIM/ MMAX, NMAX, IDMAX    doesn't seem to be used
#      EQUIVALENCE (IXX1(1),XEIG1(1)),(IXX2(1),XEIG2(1))  diff types,
#      EQUIVALENCE (IXX3(1),XEIG3(1)),(IXX4(1),XEIG4(1))  not done.
#      EQUIVALENCE (IYY1(1),YEIG1(1)),(IYY2(1),YEIG2(1))
#      EQUIVALENCE (IYY3(1),YEIG3(1)),(IYY4(1),YEIG4(1))
#      EQUIVALENCE (IX4(1),I{(1))   replaced all iend with ix4
#      EQUIVALENCE (IY4(1),V(1)),(JNFLAG(1),IDAT(1))   not done

  my $MMAX=0; my $NMIN=0; my $NMAX=0; my $IDMAX=0; my $IT=0;
  my $IWEIGH=0; my $IRESC=0; my $IPUNCH=0; my $MI=0; my $N=0;
  my $NID=0; my $ID=0; my $I=0; my $J=0; my $ID1=0; my $ID2=0;
  my $ITEND=0; my $JJ=0;
  my $AXX=0.0; my $SHORT=0.0; my $AIJ=0.0; my $BTOP=0.0;
  my $BBOT=0.0; my $ATOP=0.0; my $ABOT=0.0; my $AMAX=0.0;
  my $AMIN=0.0; my $TOT=0.0; my $EIG1=0.0; my $EIG2=0.0;
  my $EIG3=0.0; my $EIG4=0.0;
  my $ok1=0; my $ok2=0; my $ok3=0; my $ok4=0;
  my @BEFORE=(); my @AFTER=();
  
  my $IRA=0; my $MK=0;

#  xeig1=vararraycreate([1,dcaN], vardouble);
#  xeig2=vararraycreate([1,dcaN], vardouble);
#  xeig3=vararraycreate([1,dcaN], vardouble);
#  xeig4=vararraycreate([1,dcaN], vardouble);
#  aidot=vararraycreate([1,dcaN], vardouble);
#  iend=vararraycreate([1,dcaN], varinteger);
#  ix1=vararraycreate([1,dcaN], varDouble);
#  ix2=vararraycreate([1,dcaN], varDouble);
#  ix3=vararraycreate([1,dcaN], varDouble);
#  ix4=vararraycreate([1,dcaN], varDouble);
#  ixx1=vararraycreate([1,dcaN], varinteger);
# ixx2=vararraycreate([1,dcaN], varinteger);
#  ixx3=vararraycreate([1,dcaN], varinteger);
#  ixx4=vararraycreate([1,dcaN], varinteger);
#  inflag=vararraycreate([1,dcaN], varinteger);
#  y2=vararraycreate([1,dcaM], vardouble);
#  y3=vararraycreate([1,dcaM], vardouble);
#  y4=vararraycreate([1,dcaM], vardouble);
#  y5=vararraycreate([1,dcaM], vardouble);
#  v=vararraycreate([1,dcaM], vardouble);
#  yeig1=vararraycreate([1,dcaM], vardouble);
#  yeig2=vararraycreate([1,dcaM], vardouble);
#  yeig3=vararraycreate([1,dcaM], vardouble);
#  yeig4=vararraycreate([1,dcaM], vardouble);
#  adotj=vararraycreate([1,dcaM], vardouble);
#  jnflag=vararraycreate([1,dcaM], varinteger);
# iy1=vararraycreate([1,dcaM], varDouble);
#  iy2=vararraycreate([1,dcaM], varDouble);
#  iy3=vararraycreate([1,dcaM], varDouble);
#  iy4=vararraycreate([1,dcaM], varDouble);
#  iyy1=vararraycreate([1,dcaM], varinteger);
#  iyy2=vararraycreate([1,dcaM], varinteger);
# iyy3=vararraycreate([1,dcaM], varinteger);
#  iyy4=vararraycreate([1,dcaM], varinteger);
#  qidat=vararraycreate([1,dcaN*dcaM], vardouble);
#  idat=vararraycreate([1,dcaN*dcaM], varinteger);
  
  my @XEIG1=(); my @XEIG2=(); my @XEIG3=(); my @XEIG4=(); my @AIDOT=(); my @IEND=();
  my @IX1=(); my @IX2=(); my @IX3=(); my @IX4=(); my @IXX1=(); my @IXX2=(); my @IXX3=();
  my @IXX4=();  my @INFLAG=(); my @Y2=(); my @Y3=(); my @Y4=(); my @Y5=(); my @V=();
  my @YEIG1=(); my @YEIG2=(); my @YEIG3=(); my @YEIG4=(); my @ADOTJ=(); my @JNFLAG=();
  my @IY1=(); my @IY2=(); my @IY3=(); my @IY4=(); my @IYY1=(); my @IYY2=(); my @IYY3=();
  my @IYY4=(); my @QIDAT=(); my @IDAT=();
  my @dat=(); my $data=\@dat;

  $PAST::DCA::ERR=0;
#  $dcaM=high(DCARR); # Objects
#  $dcaN=high(dcarr[0]); # Variables
#  if (($dcaM<2) or ($dcaN<4)) {
#    print("At least 2 rows and 4 columns required");
#    return;
#  }
  $IRA=0; # DCA. IRA=1 gives reciprocal averaging.
  $MK=26; # Segments
  
  # Transpose incoming matrix
  for $I (0 .. $#{@{$data_trans}}) {
    for $J (0 .. $#{${@{$data_trans}}[$I]}) { ${$data}[$J][$I]=${$data_trans}[$I][$J]; }
  }
  
  $PAST::DCA::dcaM=$#{@{$data}}+1; # Number of rows. Now extend the matrix to rectangle
  my $taxmax=0;
  for $I ( 0 .. $PAST::DCA::dcaM-1 ) {
    if ($#{${@{$data}}[$I]} > $taxmax) { $taxmax=$#{${@{$data}}[$I]}; }
  }
  $PAST::DCA::dcaN=$taxmax+1;
  for $I ( 0 .. $PAST::DCA::dcaM-1 ) {
    if ($#{${@{$data}}[$I]} < $taxmax) { ${$data}[$I][$taxmax]=0; }
  }  

  $MMAX=$PAST::DCA::dcaN;
  $NMAX=$PAST::DCA::dcaM;
  $IDMAX=32768; # IDMAX=dcaN*dcaM;


# Code to get names of input and output files and open them deleted

  $IT=1;
  $AXX=0.0;

# Code for (transformation (what is that?) deleted

  $BEFORE[2]=0.0;
  $AFTER[2]=0.0;
  $BEFORE[3]=1.0E10;
  $AFTER[3]=1.0E10;
  $IT=3;
  $IT++;
  $BEFORE[1] = 0.0;
  $AFTER[1]= $AFTER[2];
  $BEFORE[$IT]=1.0E10;
  $AFTER[$IT]=$AFTER[$IT-1];

  $IWEIGH=0; # No downweighting of rare species (set to 1 for (yes)

  $IRESC=4; # Rescaling of axes. 4=default, 1-20=iterations, 0=none

  # IRA=0; # 0=detrending, 1=basic reciprocal averaging
  if ($IRA!=1) { $IRA=0; }
  if ($IRA==1) { $IRESC=0; }

  #MK=0; # Number of segments (0=default)
  # NOW TAKEN FROM MKspinEdit
  if ($MK==0) { $MK=26; }
  $MK=$MK+4;
  if ($MK<14) { $MK=14; }
  if ($MK>50) { $MK=50; }

  $SHORT=0; # SPECIFY RESCALING THRESHOLD - OR TYPE 0 for (DEFAULT

  $IPUNCH=0; # DO NOT WRITE SCORES TO HARDCOPY
  # JA: not sure why this was here
  #print("<br><br><br>");
  QUIKIN($MMAX,$NMAX,$IDMAX,\$MI,\$N,\@IDAT,\@QIDAT,\@IX4,\@IEND,$NID,\@INFLAG, $data);

# THIS COMPLETES THE READING IN OF THE DATA
if ($PAST::DCA::ERR==0) {
  for ($ID=1; $ID<=$NID; $ID++) {
    $AIJ=$QIDAT[$ID];
#    $IT=0;
#    repeat
#      $IT++;
#    until ($BEFORE[$IT]>$AIJ);
    $IT=1;
    while ($BEFORE[$IT]<=$AIJ) { $IT++; }

    $BTOP=$BEFORE[$IT];
    $BBOT=$BEFORE[$IT-1];
    $ATOP=$AFTER[$IT];
    $ABOT=$AFTER[$IT-1];
    $AIJ=$ABOT+($AIJ-$BBOT)*($ATOP-$ABOT)/($BTOP-$BBOT);
    if ($AIJ<1.0E-10) { $AIJ=1.0E-10; }
    $QIDAT[$ID]=$AIJ;
  }
  for ($J=1; $J<=$N; $J++) {
    $YEIG1[$J]=0.0;
    $Y2[$J]=1.0E-10;
  }
  for ($I=1; $I<=$MI; $I++) {
    $ID1=$IX4[$I];
    $ID2=$IEND[$I];
    for ($ID=$ID1; $ID<=$ID2; $ID++) {
      $J=$IDAT[$ID];
      $AIJ=$QIDAT[$ID];
      $YEIG1[$J]=$YEIG1[$J]+$AIJ;
      $Y2[$J]=$Y2[$J]+$AIJ*$AIJ;
    }
  }
  for ($J=1; $J<=$N; $J++) { $Y2[$J]=sqr($YEIG1[$J])/$Y2[$J]; }
  XMAXMI(\@Y2,\$AMAX,\$AMIN,$N);
  $AMAX=$AMAX/5.0;
  for ($J=1; $J<=$N; $J++) {
    $V[$J]=1.0;
    if (($IWEIGH==1) and ($Y2[$J]<$AMAX)) { $V[$J]=$Y2[$J]/$AMAX; }
  }

# IF THERE IS REWEIGHTING TO BE DONE THIS IS NOW ACCOMPLISHED BY
# MULTIPLYING BY V(J)
  for ($I=1; $I<=$MI; $I++) {
    $ID1=$IX4[$I];
    $ID2=$IEND[$I];
    for ($ID=$ID1; $ID<=$ID2; $ID++) {
      $J=$IDAT[$ID];
      $QIDAT[$ID]=$QIDAT[$ID]*$V[$J];
    }
  }

  for ($I=1; $I<=$MI; $I++) { $XEIG1[$I]=1.0; }
  XYMULT(\@XEIG1,\@ADOTJ,$MI,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT);
  $TOT=0.0;
  for ($J=1; $J<=$N; $J++) {
    if ($ADOTJ[$J]<1.0E-11) { $ADOTJ[$J]=1.0E-11; }
    $TOT=$TOT+$ADOTJ[$J];
  }
  for ($J=1; $J<=$N; $J++) { $YEIG1[$J]=1.0; }
  YXMULT(\@YEIG1,\@AIDOT,$MI,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT);

  $ok1=1; $ok2=1; $ok3=1; $ok4=1;
# PRELIMINARIES ARE NOW OVER.  EIGENVECTORS ARE CALCULATED.
  if (EIGY(\@XEIG1,\@YEIG1,\$EIG1,0,$IRA,$IRESC,$SHORT,
    $MI,$MK,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT,\@Y2,\@Y3,\@Y4,\@Y5,
    \@XEIG1,\@XEIG2,\@XEIG3,\@IX1,\@IX2,\@IX3,\@AIDOT,\@ADOTJ)==1) {
      print("Eigenanalysis failed for axis 1");
      $ok1=0;
  } else {
    if ($IRA==0) { CUTUP(\@XEIG1,\@IX1,$MI,$MK); }
    if (EIGY(\@XEIG2,\@YEIG2,\$EIG2,1,$IRA,$IRESC,$SHORT,
      $MI,$MK,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT,\@Y2,\@Y3,\@Y4,\@Y5,
      \@XEIG1,\@XEIG2,\@XEIG3,\@IX1,\@IX2,\@IX3,\@AIDOT,\@ADOTJ)==1) {
        print("Eigenanalysis failed for axis 2");
        $ok2=0;
    } else {
      if ($IRA==0) { CUTUP(\@XEIG2,\@IX2,$MI,$MK); }
      if (EIGY(\@XEIG3,\@YEIG3,\$EIG3,2,$IRA,$IRESC,$SHORT,
        $MI,$MK,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT,\@Y2,\@Y3,\@Y4,\@Y5,
        \@XEIG1,\@XEIG2,\@XEIG3,\@IX1,\@IX2,\@IX3,\@AIDOT,\@ADOTJ)==1) {
          print("Eigenanalysis failed for axis 3");
          $ok3=0;
      } else {
        if ($IRA==0) { CUTUP(\@XEIG3,\@IX3,$MI,$MK); }
        if (EIGY(\@XEIG4,\@YEIG4,\$EIG4,3,$IRA,$IRESC,$SHORT, # This was commented out
          $MI,$MK,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT,\@Y2,\@Y3,\@Y4,\@Y5,
          \@XEIG1,\@XEIG2,\@XEIG3,\@IX1,\@IX2,\@IX3,\@AIDOT,\@ADOTJ)==1) {
            print("Eigenanalysis failed for axis 4");
            $ok4=0;
        }
      }
    }
#  WRITE(IUOUT2,2100)
# XEIG4 har veldig store verdier her.
  }
  YXMULT(\@YEIG1,\@XEIG1,$MI,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT);
  YXMULT(\@YEIG2,\@XEIG2,$MI,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT);
  YXMULT(\@YEIG3,\@XEIG3,$MI,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT);
  YXMULT(\@YEIG4,\@XEIG4,$MI,$N,$NID,\@IX4,\@IEND,\@IDAT,\@QIDAT); # This was commented out

  for ($I=1; $I<=$MI; $I++) {
    $IX1[$I]=$XEIG1[$I]/$AIDOT[$I]; # was *100.0
    $IX2[$I]=$XEIG2[$I]/$AIDOT[$I]; # was *100.0
    $IX3[$I]=$XEIG3[$I]/$AIDOT[$I]; # was *100.0
    $IX4[$I]=$XEIG4[$I]/$AIDOT[$I]; # This was commented out
  }
  for ($J=1; $J<=$N; $J++) {
    $IY1[$J]=$YEIG1[$J]; # was *100.0
    $IY2[$J]=$YEIG2[$J]; # was *100.0
    $IY3[$J]=$YEIG3[$J]; # was *100.0
    $IY4[$J]=$YEIG4[$J]; # This was commented out
  }
#--FOLLOWING SECTION (TO STATEMENT 330) ADDED BY P.MINCHIN FEB 1988

# VECTORS IX*,IY* NOW CONTAIN THE SCORES.  REMAINDER OF PROGRAM
# PUTS THESE OUT IN APPROPRIATE FORM.
# 2100 FORMAT('1')

# Write TITLE removed

  $MK=$MK-4;

#  $IT=0;
#  repeat
#    $IT++;
#  until ($BEFORE[$IT]>=9.9E9);
  $IT=1;
  while ($BEFORE[$IT]<9.9E9) { $IT++; }

  $ITEND=$IT-1;
#      WRITE(IUOUT2, 2300) (BEFORE(IT),AFTER(IT),IT=2,IT})
# 2300 FORMAT((1X,'TRANSFORMATION',5(6X,F8.2,F8.2)))
#      WRITE(IUOUT2,2101)
# 2101 FORMAT(/1X,'SPECIES SCORES'/1X)
  $JJ=0;
  for ($J=1; $J<=$N; $J++) {
    if ($ADOTJ[$J]!=1.0E-11) {
      $JJ++;
#      JNAME1[$JJ]=JNAME1[$J];
#      JNAME2[$JJ]=JNAME2[$J];
      $IY1[$JJ]=$IY1[$J];
      $IY2[$JJ]=$IY2[$J];
      $IY3[$JJ]=$IY3[$J];
      $IY4[$JJ]=$IY4[$J];  # This was commented out
      $JNFLAG[$JJ]=$J;
    }
  }
  $N=$JJ;
  
  if ($PAST::DCA::ERR==0) {
    print("<h2 style=\"margin-left: 5em; padding-bottom: 1.5em;\">Detrended Correspondence Analysis output</h2>\n\n");

    # get scaling factors to turn the taxon scores into pixel locations
    # 0.5 is coming up over and over because the bounds are being set to be no
    #  more than a half-change beyond the range of the data
    my $maxX1;
    my $minX1 = 9999;
    my $maxX2;
    my $minX2 = 9999;
    for $I (1 .. $PAST::DCA::dcaN) {
      if ( $IX1[$I] > $maxX1 )	{
        $maxX1 = $IX1[$I];
      }
      if ( $IX1[$I] < $minX1 )	{
        $minX1 = $IX1[$I];
      }
      if ( $IX2[$I] > $maxX2 )	{
        $maxX2 = $IX2[$I];
      }
      if ( $IX2[$I] < $minX2 )	{
        $minX2 = $IX2[$I];
      }
    }
    my $X1top = int( ( 2 * $maxX1 ) + 1 ) / 2;
    my $X1bot = int( ( 2 * $minX1 ) - 1 ) / 2;
    my $scaleX1 = $X1top - $X1bot;
    my $X1bot = $X1bot;
    my $X2top = int( ( 2 * $maxX2 ) + 1 ) / 2;
    my $X2bot = int( ( 2 * $minX2 ) - 1 ) / 2;
    my $scaleX2 = $X2top - $X2bot;
    my $X2bot = $X2bot;

    # do the same thing for taxa
    my $maxY1;
    my $minY1 = 9999;
    my $maxY2;
    my $minY2 = 9999;
    for $I (1 .. $PAST::DCA::dcaM) {
      if ( $IY1[$I] > $maxY1 )	{
        $maxY1 = $IY1[$I];
      }
      if ( $IY1[$I] < $minY1 )	{
        $minY1 = $IY1[$I];
      }
      if ( $IY2[$I] > $maxY2 )	{
        $maxY2 = $IY2[$I];
      }
      if ( $IY2[$I] < $minY2 )	{
        $minY2 = $IY2[$I];
      }
    }
    my $Y1top = int( ( 2 * $maxY1 ) + 1 ) / 2;
    my $Y1bot = int( ( 2 * $minY1 ) - 1 ) / 2;
    my $scaleY1 = $Y1top - $Y1bot;
    my $Y1bot = $Y1bot;
    my $Y2top = int( ( 2 * $maxY2 ) + 1 ) / 2;
    my $Y2bot = int( ( 2 * $minY2 ) - 1 ) / 2;
    my $scaleY2 = $Y2top - $Y2bot;
    my $Y2bot = $Y2bot;

    my $plot_width = 300;
    my $plot_height = 300;
    my $border_width = 40;
    my $border_height = 40;
    my $image_width = $plot_width + (2 * $border_width);
    my $image_height = $plot_height + (2 * $border_height);

    my $im = new GD::Image($image_width,$image_height,1);
    $im->interlaced('true');
    $im->transparent(-1);
    my $im2 = new GD::Image($image_width,$image_height,1);
    $im2->interlaced('true');
    $im2->transparent(-1);

    # I'm doing all of my calculations assuming that zero on the Y-axis is
    #  on the bottom, but actually it's on the top, so reverse the Y values
    #  using this function
    sub yrev { $image_height - $_[0] }

    # declare some colors
    my $white = $im->colorAllocate(255, 255, 255);
    my $black = $im->colorAllocate(0, 0, 0);
    my $gray = $im->colorAllocate(192, 192, 192);
    my $red = $im->colorAllocate(255, 0, 0);
    my $green = $im->colorAllocate(0, 255, 0);
    my $blue = $im->colorAllocate(0, 0, 255);

    # declare the path for the font
    my $DATAFILE_DIR = $ENV{DOWNLOAD_DATAFILE_DIR};
    my $font = "$DATAFILE_DIR/fonts/intrebol.ttf";

    # draw a white background
    $im->filledRectangle(0,0,$image_width,$image_height,$white);
    $im2->filledRectangle(0,0,$image_width,$image_height,$white);

    # draw a box around the plotting area proper
    $im->rectangle($border_width,$border_height,$border_width + $plot_width,$border_height + $plot_height,$gray);
    $im2->rectangle($border_width,$border_height,$border_width + $plot_width,$border_height + $plot_height,$gray);

    # say what the data are
    $im->stringFT($black,$font,16,0,($image_width / 2) - 40,$border_height - 18,"Samples");
    $im2->stringFT($black,$font,16,0,($image_width / 2) - 30,$border_height - 18,"Taxa");

    # draw crosshairs indicating the origins
    my $originx = $border_width + ( ( ( 0 - $X1bot ) / $scaleX1 ) * $plot_width );
    my $originy = yrev( $border_height + ( ( ( 0 - $X2bot ) / $scaleX2 ) * $plot_height ) );
    $im->filledRectangle($originx - 10,$originy - 1,$originx + 10,$originy + 1,$gray);
    $im->filledRectangle($originx - 1,$originy - 10,$originx + 1,$originy + 10,$gray);
    $originx = $border_width + ( ( ( 0 - $Y1bot ) / $scaleY1 ) * $plot_width );
    $originy = yrev( $border_height + ( ( ( 0 - $Y2bot ) / $scaleY2 ) * $plot_height ) );
    $im2->filledRectangle($originx - 10,$originy - 1,$originx + 10,$originy + 1,$gray);
    $im2->filledRectangle($originx - 1,$originy - 10,$originx + 1,$originy + 10,$gray);

    # samples are blue circles
    my @xsample;
    my @ysample;
    for $I (1 .. $PAST::DCA::dcaN) {
      my $x = $border_width + ( ( ( $IX1[$I] - $X1bot ) / $scaleX1 ) * $plot_width );
      my $y = yrev( $border_height + ( ( ( $IX2[$I] - $X2bot ) / $scaleX2 ) * $plot_height ) );
      $im->filledEllipse($x,$y,7,7,$blue);
      push @xsample , $x;
      push @ysample , $y;
    }
    my @xtaxon;
    my @ytaxon;
    # taxa are red squares
    for $I (1 .. $PAST::DCA::dcaM) {
      my $x = $border_width + ( ( ( $IY1[$I] - $Y1bot ) / $scaleY1 ) * $plot_width );
      my $y = yrev( $border_height + ( ( ( $IY2[$I] - $Y2bot ) / $scaleY2 ) * $plot_height ) );
      $im2->filledRectangle($x - 2,$y - 2,$x + 2,$y + 2,$red);
      push @xtaxon , $x;
      push @ytaxon , $y;
    }

    # axis legends
    $im->stringFT($black,$font,12,0,($image_width / 2) - 21,$border_height + $plot_height + 33,"Axis 1");
    $im->stringFT($black,$font,12,1.57,$border_width - 27,($image_height / 2 ) + 22,"Axis 2");
    $im2->stringFT($black,$font,12,0,($image_width / 2) - 21,$border_height + $plot_height + 33,"Axis 1");
    $im2->stringFT($black,$font,12,1.57,$border_width - 27,($image_height / 2 ) + 22,"Axis 2");

    # tick marks and values
    for my $i ($X1bot*2..$X1top*2)	{
      my $x = $border_width + ( ( ( ( $i / 2 ) - $X1bot ) / $scaleX1 ) * $plot_width );
      $im->line($x,$border_height + $plot_height,$x,$border_height + $plot_height + 5,$gray);
      my $tickval = sprintf "%.1f",$i / 2;
      $im->stringFT($black,$font,6,0,$x - 6,$border_height + $plot_height + 15,$tickval);
    }
    for my $i ($X2bot*2..$X2top*2)	{
      my $y = yrev( $border_height + ( ( ( ( $i / 2 ) - $X2bot ) / $scaleX2 ) * $plot_height) );
      $im->line($border_width,$y,$border_width - 5,$y,$gray);
      my $tickval = sprintf "%.1f",$i / 2;
      $im->stringFT($black,$font,6,1.57,$border_width - 10,$y + 8,$tickval);
    }
    for my $i ($Y1bot*2..$Y1top*2)	{
      my $x = $border_width + ( ( ( ( $i / 2 ) - $Y1bot ) / $scaleY1 ) * $plot_width );
      $im2->line($x,$border_height + $plot_height,$x,$border_height + $plot_height + 5,$gray);
      my $tickval = sprintf "%.1f",$i / 2;
      $im2->stringFT($black,$font,6,0,$x - 6,$border_height + $plot_height + 15,$tickval);
    }
    for my $i ($Y2bot*2..$Y2top*2)	{
      my $y = yrev( $border_height + ( ( ( ( $i / 2 ) - $Y2bot ) / $scaleY2 ) * $plot_height) );
      $im2->line($border_width,$y,$border_width - 5,$y,$gray);
      my $tickval = sprintf "%.1f",$i / 2;
      $im2->stringFT($black,$font,6,1.57,$border_width - 10,$y + 8,$tickval);
    }

    my $GIF_DIR = $ENV{MAP_GIF_DIR};
    my $GIF_HTTP_ADDR = "/public/maps";
    binmode STDOUT;
    open DCA_PNG1,">$GIF_DIR/dca_plot1.png";
    print DCA_PNG1 $im->png;
    close DCA_PNG1;
    chmod 0664, "$GIF_DIR/dca_plot1.png";
    open DCA_PNG2,">$GIF_DIR/dca_plot2.png";
    print DCA_PNG2 $im2->png;
    close DCA_PNG2;
    chmod 0664, "$GIF_DIR/dca_plot2.png";


    print "<div style=\"width: 760px;\">\n";
    print "<div style=\"float: left;\"><img src=\"$GIF_HTTP_ADDR/dca_plot1.png\"></div>\n";
    for $I (1 .. $PAST::DCA::dcaN) {
      # the extra four pixels have to do with how the point is rendered
      my $leftpos = int($xsample[$I-1] - 4 - $image_width) . "px";
      # the point is 12 pixels down from dead center, so you need to move it up
      my $toppos = int($ysample[$I-1] - 12) . "px";
      # it's extremely important to balance out left with margin-right, or
      #  you get bizarre formatting errors
      print "<div id=\"PAST_point\" style=\"float: left; position: relative; left: $leftpos; top: $toppos; margin-right: $leftpos; color: #0000FF;\">.<ul><li style=\"border-left: 1px solid gray; padding-left: 3px;\"><div name=\"collection_no\" class=\"visiblePASTdata tiny\" style=\"visibility: visible;\">$rows[$I-1]</div>";

      for my $ef ( @extra_fields )	{
        print "<div name=\"$ef\" class=\"hiddenPASTdata tiny\" style=\"visibility: hidden;\">$extra_data{$rows[$I-1]}{$ef}</div>";
      }

      print "</li></ul></div>\n";
    }

    print "\n<div style=\"float: left;\"><img src=\"$GIF_HTTP_ADDR/dca_plot2.png\"></div>\n";
    for $I (1 .. $PAST::DCA::dcaM) {
      my $leftpos = int($xtaxon[$I-1] - 3 - $image_width) . "px";
      my $toppos = int($ytaxon[$I-1] - 12) . "px";
      print "<div id=\"PAST_point\" style=\"float: left; position: relative; left: $leftpos; top: $toppos; margin-right: $leftpos; color: #FF0000;\">.<ul><li>$columns[$I-1]</li></ul></div>\n";
    }
    print "</div>\n";

    # without this clear, you can't get the tables to go beneath the figures
    print "\n<div style=\"clear: left; padding-top: 2em;\">\n\n";

    print "<p class=\"medium\">Move your mouse over each point to display information about it.</p>\n";
    if ( $#extra_fields > 0 )	{

      print "<p class=\"medium\">Check the additional fields you want to display for the samples (= collections):</p>\n";
      print "<form>\n";
      print "<input type=\"checkbox\" class=\"point_data_to_show\" value=\"Y\" onClick=\"showHide('collection_no');\" checked> collection number</input>\n";
      for my $ef ( @extra_fields )	{
        print "<input type=\"checkbox\" class=\"point_data_to_show\" value=\"Y\" onClick=\"showHide('$ef');\">$ef</input>\n";
      }
      print "</form>\n";
    }

    print "<p></p>\n<hr>\n\n";

    print "<h3>Eigenvalues</h3>
    <div class=\"small\">
    <table class=\"PASTtable\">
    <tr><td class=\"PASTheader\">Axis 1</td><td class=\"PASTcell\">".sprintf("%.4f", $EIG1)."</td></tr>
    <tr><td class=\"PASTheader\">Axis 2</td><td class=\"PASTcell\">".sprintf("%.4f", $EIG2)."</td></tr>
    <tr><td class=\"PASTheader\">Axis 3</td><td class=\"PASTcell\">".sprintf("%.4f", $EIG3)."</td></tr>
    <tr><td class=\"PASTheader\">Axis 4</td><td class=\"PASTcell\">".sprintf("%.4f", $EIG4)."</td></tr>
    </table>
    </div>
    </div>\n";

    print "
    <div style=\"float: left;\">
    <h3>Sample scores</h3>
    <div class=\"small\">
    <table class=\"PASTtable\">
    <tr><td class=\"PASTheader\">&nbsp;</td><td class=\"PASTheader\">Axis 1</td>
    <td class=\"PASTheader\">Axis 2</td><td class=\"PASTheader\">Axis 3</td></tr>\n";
    for $I (1 .. $PAST::DCA::dcaN) {
      print "<tr><td class=\"PASTheader\">".$rows[$I-1]."</td>\n";
      print "<td class=\"PASTcell\">".sprintf("%.4f",$IX1[$I])."</td><td class=\"PASTcell\">".sprintf("%.4f",$IX2[$I])."</td><td class=\"PASTcell\">".sprintf("%.4f",$IX3[$I])."</td></tr>\n";
    }
    print "</table>
    </div>
    </div>\n";

    print "<div style=\"float: left; margin-left: 2em;\">
    <h3>Taxon scores</h3>
    <div class=\"small\">
    <table class=\"PASTtable\">
    <tr><td class=\"PASTheader\">&nbsp;</td><td class=\"PASTheader\">Axis 1</td>
    <td class=\"PASTheader\">Axis 2</td><td class=\"PASTheader\">Axis 3</td></tr>\n";
    for $I (1 .. $PAST::DCA::dcaM) {
      print "<tr><td class=\"PASTheader\">".$columns[$I-1]."</td>\n";
      print "<td class=\"PASTcell\">".sprintf("%.4f",$IY1[$I])."</td><td class=\"PASTcell\">".sprintf("%.4f",$IY2[$I])."</td><td class=\"PASTcell\">".sprintf("%.4f",$IY3[$I])."</td></tr>\n";
    }
    print "</table>
    </div>
    </div>\n";

    print "</div>\n";
  }
  
#  e1label.caption=floattostrf($EIG1, ffGeneral, 4, 2);
#  e2label.caption=floattostrf($EIG2, ffGeneral, 4, 2);
#  e3label.caption=floattostrf($EIG3, ffGeneral, 4, 2);
#  e4label.caption=floattostrf($EIG4, ffGeneral, 4, 2);
#      STOP
} # if err==0
} # sub DCA

#sub dcascatplot;
#var vec1, vec2: TarrayD;
#{
#  my $i=0; my $j=0; my $M=0; my $N=0;

#  if (axisCHeck.checked) {
#    grc^.xlabeltext='Axis 2'; grc^.ylabeltext='Axis 3';
#  } else {
#    grc^.xlabeltext='Axis 1'; grc^.ylabeltext='Axis 2';
#  };
#  $M=vararrayhighbound(dcarr,1); # Num points
#  $N=vararrayhighbound(dcarr,2); # Num dims

#  # Samples
#  if ((dotcheckbox.checked) or (labelcheckbox.checked)) {
#    setlength(vec1,$N+1); # Axis1 values
#    setlength(vec2,$N+1); # Axis2 values
#    for ($i=1; $i<=$N; $i++) {
#      if (axisCheck.checked) {
#        vec1[$i]=$ix2[$i]; vec2[$i]=$ix3[$i];
#      } else {
#        vec1[$i]=$ix1[$i]; vec2[$i]=$ix2[$i];
#      };
#    };
#    if (dotcheckbox.checked) {
#      drawxy_aspoints(grc, vec1, vec2, clBlue, PsSolid); }
#    if (labelcheckbox.checked) {
#      drawxy_aslabels(grc, vec1, vec2, clBlue, dcaleft); }
#    finalize(vec1); finalize(vec2);
#  };

#  # Species
#  if ((rowcheckbox.checked) or (rowlabelcheck.checked)) {
#    setlength(vec1,$M+1); # Comp1 values
#    setlength(vec2,$M+1); # Comp2 values
#    for ($i=1; $i<=$M; $i++) {
#      if (axisCheck.checked) {
#        vec1[$i]=$iy2[$i]; vec2[$i]=$iy3[$i];
#      } else {
#        vec1[$i]=$iy1[$i]; vec2[$i]=$iy2[$i];
#      };
#    };
#    if (rowcheckbox.checked) { drawxy_assymbols(grc, vec1, vec2, dcatop); }
#    if (rowlabelcheck.checked) {
#      drawxy_aslabels_row(grc, vec1, vec2, clBlack, dcatop); }
#    if (ellipseCheck.checked) { drawxy_asellipses(grc, vec1, vec2, dcatop); }
#    if (convexCheck.checked) { drawxy_asconvexhulls(grc, vec1, vec2, dcatop); }
#    finalize(vec1); finalize(vec2);
#  }

#}

1;

